#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusCode {
    Ok = 0,
    NotLeader = 1,
    NotFound = 2,
    AlreadyExists = 3,
    InvalidArg = 4,
    IOError = 5,
    Timeout = 6,
    Internal = 7,
    LeaseExpired = 8,
    Compacted = 9,
}

#[derive(Debug, Clone)]
pub struct Status {
    code: StatusCode,
    msg: String,
    leaderhint: u64,
}

impl Status {
    pub fn ok() -> Self {
        Status { code: StatusCode::Ok, msg: String::new(), leaderhint: 0 }
    }

    pub fn notleader(hint: u64) -> Self {
        Status { code: StatusCode::NotLeader, msg: "not leader".into(), leaderhint: hint }
    }

    pub fn notfound(msg: impl Into<String>) -> Self {
        Status { code: StatusCode::NotFound, msg: msg.into(), leaderhint: 0 }
    }

    pub fn invalidarg(msg: impl Into<String>) -> Self {
        Status { code: StatusCode::InvalidArg, msg: msg.into(), leaderhint: 0 }
    }

    pub fn ioerror(msg: impl Into<String>) -> Self {
        Status { code: StatusCode::IOError, msg: msg.into(), leaderhint: 0 }
    }

    pub fn timeout(msg: impl Into<String>) -> Self {
        Status { code: StatusCode::Timeout, msg: msg.into(), leaderhint: 0 }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Status { code: StatusCode::Internal, msg: msg.into(), leaderhint: 0 }
    }

    pub fn leaseexpired() -> Self {
        Status { code: StatusCode::LeaseExpired, msg: "lease expired".into(), leaderhint: 0 }
    }

    pub fn isok(&self) -> bool { self.code == StatusCode::Ok }
    pub fn isnotleader(&self) -> bool { self.code == StatusCode::NotLeader }
    pub fn isnotfound(&self) -> bool { self.code == StatusCode::NotFound }
    pub fn istimeout(&self) -> bool { self.code == StatusCode::Timeout }

    pub fn code(&self) -> StatusCode { self.code }
    pub fn message(&self) -> &str { &self.msg }
    pub fn leaderhint(&self) -> u64 { self.leaderhint }
}
