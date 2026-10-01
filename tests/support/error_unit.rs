use super::*;
#[test]
fn adapter_errors_keep_only_coarse_static_context() {
    for (kind, io) in [
        (ErrorKind::InvalidAddress, std::io::ErrorKind::InvalidInput),
        (ErrorKind::Unsupported, std::io::ErrorKind::Unsupported),
        (ErrorKind::Network, std::io::ErrorKind::NotConnected),
        (ErrorKind::Unreachable, std::io::ErrorKind::NotConnected),
        (ErrorKind::Timeout, std::io::ErrorKind::TimedOut),
        (ErrorKind::Closed, std::io::ErrorKind::BrokenPipe),
        (ErrorKind::Limit, std::io::ErrorKind::OutOfMemory),
        (ErrorKind::Protocol, std::io::ErrorKind::InvalidData),
    ] {
        let error = Error::new(kind, "opaque transport failure");
        assert_eq!(error.context(), "opaque transport failure");
        assert!(error.to_string().ends_with(": opaque transport failure"));
        let adapted: std::io::Error = error.into();
        assert_eq!(adapted.kind(), io);
        assert_eq!(adapted.to_string(), error.to_string());
        assert!(!crate::MeshError::from(error).to_string().is_empty());
    }
}
