use aws_sdk_sts::config::Region;
use object_store::aws::AmazonS3Builder;

use crate::error::Error;

pub async fn get_s3(
    region: &str,
    id_token: &str,
    cloud_account_id: &str,
    role: &str,
) -> Result<AmazonS3Builder, Error> {
    let conf = aws_config::SdkConfig::builder()
        .region(Region::new(region.to_string()))
        .build();
    let sts = aws_sdk_sts::Client::new(&conf);

    let role_arn = "arn:aws:iam::".to_string() + cloud_account_id + ":role/dashbook-" + role;

    let creds = sts
        .assume_role_with_web_identity()
        .set_role_arn(Some(role_arn))
        .set_web_identity_token(Some(id_token.to_string()))
        .set_role_session_name(Some(role.to_string()))
        .send()
        .await?;

    let creds = creds.credentials().ok_or(Error::NoRoleTokens(
        role.to_string(),
        "credentials".to_string(),
    ))?;

    Ok(AmazonS3Builder::new()
        .with_region(region)
        .with_access_key_id(creds.access_key_id.as_ref().ok_or(Error::NoRoleTokens(
            role.to_string(),
            "access key id".to_string(),
        ))?)
        .with_secret_access_key(creds.secret_access_key.as_ref().ok_or(Error::NoRoleTokens(
            role.to_string(),
            "secret access key".to_string(),
        ))?)
        .with_token(creds.session_token.as_ref().ok_or(Error::NoRoleTokens(
            role.to_string(),
            "session token".to_string(),
        ))?))
}
