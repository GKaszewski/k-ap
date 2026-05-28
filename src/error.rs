use std::fmt::{Display, Formatter};

use axum::http::StatusCode;

#[derive(Debug)]
pub struct Error(pub(crate) anyhow::Error, pub(crate) StatusCode);

impl Error {
    pub fn not_found(e: impl Into<anyhow::Error>) -> Self {
        Self(e.into(), StatusCode::NOT_FOUND)
    }

    pub fn bad_request(e: impl Into<anyhow::Error>) -> Self {
        Self(e.into(), StatusCode::BAD_REQUEST)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl<T> From<T> for Error
where
    T: Into<anyhow::Error>,
{
    fn from(t: T) -> Self {
        Error(t.into(), StatusCode::INTERNAL_SERVER_ERROR)
    }
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = self.1;
        // Always log the real error internally; never expose it to the client.
        if status.is_server_error() {
            tracing::error!(error = %self.0, status = status.as_u16(), "federation error");
        } else {
            tracing::debug!(error = %self.0, status = status.as_u16(), "federation client error");
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
