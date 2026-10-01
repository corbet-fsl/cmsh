//! Bounded, cancellable frame lifecycle. Payloads have no application meaning.
use crate::{BoxFuture, MaybeSend, MaybeSync, frame};
use bytes::Bytes;
use cmsh_api::{Error, ErrorKind};
use futures::{
    FutureExt, SinkExt, StreamExt,
    io::{AsyncRead, AsyncWrite},
};
use std::{future::Future, sync::Arc};

/// Injected monotonic clock in milliseconds. Sleep must wake at the deadline.
pub trait Clock: MaybeSend + MaybeSync {
    /// Read the current monotonic tick.
    fn now(&self) -> u64;
    /// Wait until a tick, without blocking the executor.
    fn sleep_until(&self, deadline: u64) -> BoxFuture<'_, ()>;
}

/// Stream limits. One receive and one send operation at a time, with no queue.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Largest accepted payload, in `1..=frame::MAX_FRAME_BYTES`.
    pub max_frame_bytes: usize,
    /// Deadline for an entire frame, including flush, in `1..=60000` ms.
    pub timeout_ms: u64,
}

/// Observable lifecycle; cancelled in-flight operations leave `Failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Ready for a frame.
    Open,
    /// Clean EOF or completed finish.
    Closed,
    /// Cancelled, malformed, expired or broken; cannot be reused.
    Failed,
}

/// Owned framing over the frozen `futures-io` byte-stream port.
///
/// Dropping a send/read/finish future drops the owned I/O. Unknown partial
/// writes can never be retried on this stream. Success is only local I/O
/// completion, never durable peer acceptance or a delivery acknowledgement.
pub struct FramedStream<S> {
    io: Option<frame::Framed<S>>,
    clock: Arc<dyn Clock>,
    limits: Limits,
    last_tick: u64,
    state: State,
}

const CLOSED: Error = Error::new(ErrorKind::Closed, "stream closed");
const EXPIRED: Error = Error::new(ErrorKind::Timeout, "stream deadline or clock regression");
const BROKEN: Error = Error::new(ErrorKind::Protocol, "frame I/O failed");

impl<S: AsyncRead + AsyncWrite + Unpin> FramedStream<S> {
    /// Open a bounded stream. There are no spawned tasks or implicit clocks.
    pub fn open(io: S, limits: Limits, clock: Arc<dyn Clock>) -> Result<Self, Error> {
        if limits.timeout_ms == 0 || limits.timeout_ms > 60_000 {
            return Err(Error::new(ErrorKind::Limit, "invalid stream limits"));
        }
        let mut framed = frame::framed(io, limits.max_frame_bytes)?;
        framed.set_backpressure_boundary(limits.max_frame_bytes + 4);
        Ok(Self {
            io: Some(framed),
            last_tick: clock.now(),
            clock,
            limits,
            state: State::Open,
        })
    }

    /// Current state, derived from ownership while an operation is in flight.
    pub fn state(&self) -> State {
        self.state
    }

    /// Send one opaque frame and flush before releasing backpressure.
    pub async fn send_frame(&mut self, payload: &[u8]) -> Result<(), Error> {
        let mut io = self.take()?;
        if payload.len() > self.limits.max_frame_bytes {
            return Err(Error::new(ErrorKind::Limit, "frame size out of bounds"));
        }
        let bytes = Bytes::copy_from_slice(payload);
        within(
            &*self.clock,
            &mut self.last_tick,
            self.limits.timeout_ms,
            async { io.send(bytes).await.map_err(|_| BROKEN) },
        )
        .await?;
        self.io = Some(io);
        self.state = State::Open;
        Ok(())
    }

    /// Return one complete frame or clean EOF. Never returns a partial frame.
    pub async fn next_frame(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let mut io = self.take()?;
        let frame = within(
            &*self.clock,
            &mut self.last_tick,
            self.limits.timeout_ms,
            async { io.next().await.transpose().map_err(|_| BROKEN) },
        )
        .await?;
        if let Some(frame) = frame {
            self.io = Some(io);
            self.state = State::Open;
            Ok(Some(frame.to_vec()))
        } else {
            self.state = State::Closed;
            Ok(None)
        }
    }

    /// Flush, signal EOF and release the stream. A dropped future fails closed.
    pub async fn finish(&mut self) -> Result<(), Error> {
        let mut io = self.take()?;
        // Draining is represented by the operation itself. Keep Failed as the
        // stored state until completion so cancellation is unambiguous.
        within(
            &*self.clock,
            &mut self.last_tick,
            self.limits.timeout_ms,
            async { io.close().await.map_err(|_| BROKEN) },
        )
        .await?;
        self.state = State::Closed;
        Ok(())
    }

    /// Immediately drop buffered bytes and the transport, without a retry.
    pub fn cancel(&mut self) {
        self.io = None;
        self.state = State::Failed;
    }

    fn take(&mut self) -> Result<frame::Framed<S>, Error> {
        let io = self.io.take().ok_or(CLOSED)?;
        self.state = State::Failed;
        Ok(io)
    }
}

async fn within<T>(
    clock: &dyn Clock,
    last: &mut u64,
    timeout: u64,
    operation: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    let start = clock.now();
    if start < *last {
        return Err(EXPIRED);
    }
    *last = start;
    let deadline = start.checked_add(timeout).ok_or(EXPIRED)?;
    let timer = clock.sleep_until(deadline).fuse();
    let operation = operation.fuse();
    futures::pin_mut!(timer, operation);
    let result = futures::select_biased! {
        _ = timer => Err(EXPIRED),
        result = operation => result,
    };
    let end = clock.now();
    if end < start || end >= deadline {
        return Err(EXPIRED);
    }
    *last = end;
    result
}
