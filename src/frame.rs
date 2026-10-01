//! Message framing: a 4-byte big-endian length prefix, via `tokio-util`'s
//! `LengthDelimitedCodec` (no own framing code).
//!
//! The wire format matches the former `cmsg` frame codec (u32 big-endian
//! length, then payload), with one difference: empty frames are valid here.
use bytes::{Bytes, BytesMut};
use cmsh_api::{Error, ErrorKind};
use futures::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};

/// Largest frame payload the facade allows (1 MiB).
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

const LIMIT: Error = Error::new(ErrorKind::Limit, "frame size out of bounds");
const MALFORMED: Error = Error::new(ErrorKind::Protocol, "malformed frame");
const CLOSED: Error = Error::new(ErrorKind::Closed, "frame codec closed");

/// The length-delimited codec with a payload bound of `1..=MAX_FRAME_BYTES`.
pub fn codec(max_frame_bytes: usize) -> Result<BoundedCodec, Error> {
    if max_frame_bytes == 0 || max_frame_bytes > MAX_FRAME_BYTES {
        return Err(LIMIT);
    }
    Ok(BoundedCodec {
        inner: LengthDelimitedCodec::builder()
            .length_field_length(4)
            .big_endian()
            .max_frame_length(max_frame_bytes)
            .new_codec(),
        partial: false,
    })
}

/// Maintained length codec with explicit truncated-header/payload EOF tracking.
#[derive(Debug)]
pub struct BoundedCodec {
    inner: LengthDelimitedCodec,
    partial: bool,
}

impl Decoder for BoundedCodec {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let before = src.len();
        let result = self.inner.decode(src)?;
        self.partial = if result.is_some() {
            false
        } else {
            self.partial || before != src.len() || !src.is_empty()
        };
        Ok(result)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let frame = self.decode(src)?;
        if frame.is_none() && self.partial {
            return Err(MALFORMED.into());
        }
        Ok(frame)
    }
}

impl Encoder<Bytes> for BoundedCodec {
    type Error = std::io::Error;
    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

/// A framed byte stream: a `Sink<Bytes>` and a `Stream` of `BytesMut` frames.
pub type Framed<S> = tokio_util::codec::Framed<Compat<S>, BoundedCodec>;

/// Frame any `futures-io` byte stream (for example a [`crate::Substream`]).
pub fn framed<S: AsyncRead + AsyncWrite>(
    stream: S,
    max_frame_bytes: usize,
) -> Result<Framed<S>, Error> {
    Ok(tokio_util::codec::Framed::new(
        stream.compat(),
        codec(max_frame_bytes)?,
    ))
}

/// Push-based framing without I/O, for byte streams driven from outside Rust
/// (the browser bindings). Any malformed or oversized frame closes the codec.
#[derive(Debug)]
pub struct FrameCodec {
    codec: BoundedCodec,
    max_frame_bytes: usize,
    buffer: BytesMut,
    closed: bool,
}

impl FrameCodec {
    /// A codec bounded to `max_frame_bytes` payload bytes per frame.
    pub fn new(max_frame_bytes: usize) -> Result<Self, Error> {
        Ok(Self {
            codec: codec(max_frame_bytes)?,
            max_frame_bytes,
            buffer: BytesMut::new(),
            closed: false,
        })
    }

    /// Encode one frame.
    pub fn encode(&mut self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        if self.closed {
            return Err(CLOSED);
        }
        if payload.len() > self.max_frame_bytes {
            self.close();
            return Err(LIMIT);
        }
        let mut wire = BytesMut::with_capacity(payload.len() + 4);
        if self
            .codec
            .encode(Bytes::copy_from_slice(payload), &mut wire)
            .is_err()
        {
            self.close();
            return Err(LIMIT);
        }
        Ok(wire.to_vec())
    }

    /// Feed received bytes; returns every frame completed by them. A chunk may
    /// be at most one maximal frame (`max_frame_bytes + 4` bytes).
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        if self.closed {
            return Err(CLOSED);
        }
        if chunk.len() > self.max_frame_bytes + 4 {
            self.close();
            return Err(LIMIT);
        }
        self.buffer.extend_from_slice(chunk);
        let mut frames = Vec::new();
        loop {
            match self.codec.decode(&mut self.buffer) {
                Ok(Some(frame)) => frames.push(frame.to_vec()),
                Ok(None) => return Ok(frames),
                Err(_) => {
                    self.close();
                    return Err(MALFORMED);
                }
            }
        }
    }

    /// End of stream: valid only between complete frames. Closes the codec.
    pub fn finish(&mut self) -> Result<(), Error> {
        let complete = !self.closed && !self.codec.partial && self.buffer.is_empty();
        self.close();
        if complete { Ok(()) } else { Err(MALFORMED) }
    }

    /// Discard buffered bytes and refuse further use.
    pub fn close(&mut self) {
        self.closed = true;
        self.buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_in_arbitrary_chunks() {
        let mut sender = FrameCodec::new(128).unwrap();
        let mut receiver = FrameCodec::new(128).unwrap();
        let mut wire = sender.encode(b"hello").unwrap();
        wire.extend(sender.encode(b"").unwrap());
        wire.extend(sender.encode(&[7; 128]).unwrap());
        assert_eq!(&wire[..4], &5u32.to_be_bytes());
        let mut frames = Vec::new();
        for chunk in wire.chunks(3) {
            frames.extend(receiver.push(chunk).unwrap());
        }
        assert_eq!(frames, vec![b"hello".to_vec(), vec![], vec![7; 128]]);
        assert!(receiver.finish().is_ok());
    }

    #[test]
    fn oversized_frames_close_the_codec() {
        let mut codec = FrameCodec::new(16).unwrap();
        assert_eq!(codec.encode(&[0; 17]).unwrap_err().kind(), ErrorKind::Limit);
        assert_eq!(codec.encode(b"x").unwrap_err().kind(), ErrorKind::Closed);

        let mut receiver = FrameCodec::new(16).unwrap();
        let header = 17u32.to_be_bytes();
        assert_eq!(
            receiver.push(&header).unwrap_err().kind(),
            ErrorKind::Protocol
        );
        assert_eq!(receiver.push(b"x").unwrap_err().kind(), ErrorKind::Closed);
    }

    #[test]
    fn truncated_stream_is_an_error() {
        let mut codec = FrameCodec::new(16).unwrap();
        assert!(codec.push(&[0, 0, 0, 4, 1]).unwrap().is_empty());
        assert_eq!(codec.finish().unwrap_err().kind(), ErrorKind::Protocol);
    }

    #[test]
    fn bounds() {
        assert!(FrameCodec::new(0).is_err());
        assert!(FrameCodec::new(MAX_FRAME_BYTES + 1).is_err());
        assert!(FrameCodec::new(MAX_FRAME_BYTES).is_ok());
        let mut codec = FrameCodec::new(8).unwrap();
        assert_eq!(codec.push(&[0; 13]).unwrap_err().kind(), ErrorKind::Limit);
    }
}
