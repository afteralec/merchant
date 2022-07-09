use std::fmt;

#[derive(Debug)]
pub struct BrokerError {
    kind: BrokerErrorKind,
    message: String,
}

impl BrokerError {
    pub fn new(kind: BrokerErrorKind, message: &str) -> Self {
        BrokerError {
            kind,
            message: message.to_owned(),
        }
    }

    pub fn kind(&self) -> BrokerErrorKind {
        self.kind
    }
}

impl fmt::Display for BrokerError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{:?}:{}", self.kind, self.message)
    }
}

impl std::error::Error for BrokerError {}

#[derive(Debug, Clone, Copy)]
pub enum BrokerErrorKind {
    ReceiverAttached,
    SenderClosed,
}
