use std::io;

use snafu::{Location, Snafu};
use tonic::Status;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid client tls config, {}", msg))]
    InvalidTlsConfig { msg: String },

    #[snafu(display("Invalid config file path, {}", source))]
    InvalidConfigFilePath {
        source: io::Error,
        location: Location,
    },

    #[snafu(display("Failed to create gRPC channel, source: {}", source))]
    CreateChannel {
        source: tonic::transport::Error,
        location: Location,
    },

    #[snafu(display("Unknown proto column datatype: {}", datatype))]
    UnknownColumnDataType { datatype: i32, location: Location },

    #[snafu(display("Illegal GRPC client state: {}", err_msg))]
    IllegalGrpcClientState { err_msg: String, location: Location },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField { field: String, location: Location },

    // Server error carried in Tonic Status's metadata.
    #[snafu(display("{}", msg))]
    Server { status: Status, msg: String },

    #[snafu(display("Illegal Database response: {err_msg}"))]
    IllegalDatabaseResponse { err_msg: String },

    #[snafu(display("Failed to send request with streaming: {}", err_msg))]
    ClientStreaming { err_msg: String, location: Location },
}

pub type Result<T> = std::result::Result<T, Error>;

pub const INNER_ERROR_MSG: &str = "INNER_ERROR_MSG";

impl From<Status> for Error {
    fn from(e: Status) -> Self {
        fn get_metadata_value(e: &Status, key: &str) -> Option<String> {
            e.metadata()
                .get(key)
                .and_then(|v| String::from_utf8(v.as_bytes().to_vec()).ok())
        }

        let msg = get_metadata_value(&e, INNER_ERROR_MSG).unwrap_or(e.to_string());

        Self::Server { status: e, msg }
    }
}

impl Error {
    /// Indicate if the error is retriable
    pub fn is_retriable(&self) -> bool {
        !matches!(
            self,
            Self::InvalidTlsConfig { .. }
                | Self::MissingField { .. }
                | Self::InvalidConfigFilePath { .. }
        )
    }
}
