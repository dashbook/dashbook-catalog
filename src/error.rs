use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("network error")]
    Reqwest(#[from] reqwest::Error),
    #[error("object store error")]
    ObjectStore(#[from] iceberg_rust::object_store::Error),
    #[error("aws error")]
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
