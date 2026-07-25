use axum::http::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl Error {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::Unauthorized(message.into())
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden(message.into())
    }
}

impl From<activitypub_federation::error::Error> for Error {
    fn from(error: activitypub_federation::error::Error) -> Self {
        use activitypub_federation::error::Error as FedError;

        match &error {
            FedError::ActivitySignatureInvalid | FedError::ActivityBodyDigestInvalid => {
                Self::Unauthorized(error.to_string())
            }
            _ => Self::Internal(error.into()),
        }
    }
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            Error::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Error::Forbidden(_) => StatusCode::FORBIDDEN,
            Error::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        if status.is_server_error() {
            tracing::error!(error = %self, status = status.as_u16(), "federation error");
        } else {
            tracing::debug!(error = %self, status = status.as_u16(), "federation client error");
        }

        let body = match status {
            StatusCode::NOT_FOUND => "not found",
            StatusCode::BAD_REQUEST => "bad request",
            StatusCode::UNAUTHORIZED => "unauthorized",
            StatusCode::FORBIDDEN => "forbidden",
            _ => "internal server error",
        };
        (status, body).into_response()
    }
}
