use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Base64Url(#[from] base64_url::base64::DecodeError),
    #[error(transparent)]
    UTF8(#[from] std::string::FromUtf8Error),
    #[error(transparent)]
    JSON(#[from] serde_json::Error),
    #[error(transparent)]
    AWS(
        #[from]
        aws_sdk_sts::error::SdkError<
            aws_sdk_sts::operation::assume_role_with_web_identity::AssumeRoleWithWebIdentityError,
        >,
    ),
    #[error("The response for {0} didn't containt any values.")]
    EmptyResponse(String),
    #[error("No permissions for resource {0}.")]
    NoPermission(String),
    #[error("Failed to obtain {1} for role {0}.")]
    NoRoleTokens(String, String),
    #[error("{0}")]
    Other(String),
}
