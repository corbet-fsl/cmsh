//! Stream multiplexing over one backend byte stream, using `yamux`.
//!
//! [`Session`] is a cheap handle; the I/O is driven by a separate driver
//! future that the facade hands to the embedding runtime's spawner (Tokio
//! natively, `spawn_local` in the browser). No runtime is assumed here.
use crate::BoxFuture;
use cmsh_api::{BoxStream, Error, ErrorKind};
use futures::channel::{mpsc, oneshot};
use futures::io::{AsyncRead, AsyncWrite};
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};

/// Maximum concurrent substreams per connection.
pub const MAX_SUBSTREAMS: usize = 64;
/// Inbound substreams queued before `accept` picks them up; more are reset.
const INBOUND_QUEUE: usize = 16;

const CLOSED: Error = Error::new(ErrorKind::Closed, "session closed");

/// Which end of the underlying stream this side is (yamux stream id parity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The side that dialed.
    Dialer,
    /// The side that accepted.
    Listener,
}

/// Terminal session notification, emitted once across cloned handles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEvent {
    /// The driver ended or was cancelled. No message acknowledgement is implied.
    Closed,
}
struct Permit(Arc<AtomicUsize>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

enum Command {
    Open(oneshot::Sender<Result<yamux::Stream, Error>>),
    Close(oneshot::Sender<()>),
}

/// A multiplexed session: open and accept independent substreams.
#[derive(Clone)]
pub struct Session {
    commands: mpsc::Sender<Command>,
    pending: Arc<AtomicUsize>,
    events: Arc<Mutex<mpsc::Receiver<SessionEvent>>>,
    inbound: Arc<Mutex<mpsc::Receiver<yamux::Stream>>>,
}

impl Session {
    /// Wrap `io`; returns the handle and the driver future to spawn.
    /// Dropping every handle closes the session.
    pub fn new(io: BoxStream, role: Role) -> (Self, BoxFuture<'static, ()>) {
        let mut config = yamux::Config::default();
        config.set_max_num_streams(MAX_SUBSTREAMS);
        let mode = match role {
            Role::Dialer => yamux::Mode::Client,
            Role::Listener => yamux::Mode::Server,
        };
        let (commands, command_receiver) = mpsc::channel(8);
        let (inbound_sender, inbound) = mpsc::channel(INBOUND_QUEUE);
        let (event_sender, events) = mpsc::channel(1);
        let mut driver = Driver {
            event: Some(event_sender),
            connection: yamux::Connection::new(io, config, mode),
            commands: command_receiver,
            commands_done: false,
            inbound: inbound_sender,
            opening: VecDeque::new(),
            closing: Vec::new(),
        };
        let session = Self {
            commands,
            pending: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(Mutex::new(events)),
            inbound: Arc::new(Mutex::new(inbound)),
        };
        (
            session,
            Box::pin(futures::future::poll_fn(move |cx| driver.poll(cx))),
        )
    }

    /// Receive the terminal event once; subsequent calls return None.
    /// Cancelled waits leave the event available for the next caller.
    pub async fn next_event(&self) -> Option<SessionEvent> {
        self.events.lock().await.next().await
    }
    fn permit(&self) -> Result<Permit, Error> {
        // compare_exchange avoids assuming a particular async runtime semaphore.
        let mut count = self.pending.load(Ordering::SeqCst);
        loop {
            if count >= MAX_SUBSTREAMS {
                return Err(Error::new(ErrorKind::Limit, "session busy"));
            }
            match self.pending.compare_exchange(
                count,
                count + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(Permit(self.pending.clone())),
                Err(value) => count = value,
            }
        }
    }
    /// Open a new outbound substream.
    pub async fn open(&self) -> Result<Substream, Error> {
        let _permit = self.permit()?;
        let (reply, response) = oneshot::channel();
        self.commands
            .clone()
            .send(Command::Open(reply))
            .await
            .map_err(|_| CLOSED)?;
        response.await.map_err(|_| CLOSED)?.map(Substream)
    }

    /// Wait for the next inbound substream.
    pub async fn accept(&self) -> Result<Substream, Error> {
        let mut inbound = self.inbound.lock().await;
        inbound.next().await.map(Substream).ok_or(CLOSED)
    }

    /// Close the session and wait until the close is sent.
    pub async fn close(&self) -> Result<(), Error> {
        let _permit = self.permit()?;
        let (reply, done) = oneshot::channel();
        self.commands
            .clone()
            .send(Command::Close(reply))
            .await
            .map_err(|_| CLOSED)?;
        done.await.map_err(|_| CLOSED)
    }
}

struct Driver {
    event: Option<mpsc::Sender<SessionEvent>>,
    connection: yamux::Connection<BoxStream>,
    commands: mpsc::Receiver<Command>,
    commands_done: bool,
    inbound: mpsc::Sender<yamux::Stream>,
    opening: VecDeque<oneshot::Sender<Result<yamux::Stream, Error>>>,
    closing: Vec<oneshot::Sender<()>>,
}

impl Driver {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            self.opening.retain(|reply| !reply.is_canceled());
            while !self.commands_done {
                match self.commands.poll_next_unpin(cx) {
                    Poll::Ready(Some(Command::Open(reply))) => {
                        if self.opening.len() < MAX_SUBSTREAMS {
                            self.opening.push_back(reply);
                        } else {
                            let _ = reply.send(Err(Error::new(ErrorKind::Limit, "session busy")));
                        }
                    }
                    Poll::Ready(Some(Command::Close(reply))) => self.closing.push(reply),
                    // Every handle is gone: nobody can use the session any more.
                    Poll::Ready(None) => self.commands_done = true,
                    Poll::Pending => break,
                }
            }
            if self.commands_done || !self.closing.is_empty() {
                return match self.connection.poll_close(cx) {
                    Poll::Ready(_) => {
                        self.finish();
                        Poll::Ready(())
                    }
                    Poll::Pending => Poll::Pending,
                };
            }
            let mut progressed = false;
            while !self.opening.is_empty() {
                match self.connection.poll_new_outbound(cx) {
                    Poll::Ready(Ok(stream)) => {
                        progressed = true;
                        if let Some(reply) = self.opening.pop_front() {
                            // A requester that gave up drops the stream (reset).
                            let _ = reply.send(Ok(stream));
                        }
                    }
                    Poll::Ready(Err(_)) => {
                        self.finish();
                        return Poll::Ready(());
                    }
                    Poll::Pending => break,
                }
            }
            // Polling for inbound streams also drives all connection I/O.
            match self.connection.poll_next_inbound(cx) {
                Poll::Ready(Some(Ok(stream))) => {
                    progressed = true;
                    // Bounded acceptance: a full queue resets the new stream.
                    let _ = self.inbound.try_send(stream);
                }
                Poll::Ready(Some(Err(_))) | Poll::Ready(None) => {
                    self.finish();
                    return Poll::Ready(());
                }
                Poll::Pending => {}
            }
            if !progressed {
                return Poll::Pending;
            }
        }
    }

    fn finish(&mut self) {
        if let Some(mut event) = self.event.take() {
            let _ = event.try_send(SessionEvent::Closed);
        }
        for reply in self.opening.drain(..) {
            let _ = reply.send(Err(CLOSED));
        }
        for reply in self.closing.drain(..) {
            let _ = reply.send(());
        }
        self.inbound.close_channel();
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.finish();
    }
}

/// One multiplexed substream: a reliable, ordered byte stream.
pub struct Substream(yamux::Stream);

impl std::fmt::Debug for Substream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Substream")
    }
}

impl AsyncRead for Substream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl AsyncWrite for Substream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_close(cx)
    }
}
