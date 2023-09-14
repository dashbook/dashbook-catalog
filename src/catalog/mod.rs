use std::sync::Arc;

pub mod aws;

use iceberg_catalog_nessie::{apis::configuration::Configuration, catalog::NessieCatalog};
use iceberg_rust::catalog::Catalog;

use crate::{
    error::Error,
    postgrest::{get_account, get_bucket},
};

pub async fn get_catalog(
    catalog_url: &str,
    access_token: &str,
    id_token: &str,
    table_namespace: &str,
    table_name: &str,
    role: &str,
) -> Result<Arc<dyn Catalog>, Error> {
    let mut configuration = Configuration::new();
    configuration.base_path = catalog_url.to_string();
    configuration.bearer_access_token = Some(access_token.to_string());

    let catalog_name = catalog_url.split("/").last().ok_or(Error::Other(
        "Catalog url doesn't contain catalog name.".to_string(),
    ))?;

    let account = get_account(access_token, catalog_name).await?;

    let bucket = get_bucket(access_token, catalog_name, &table_namespace, &table_name).await?;

    // #[cfg(feature = "aws")]
    let object_store = aws::get_s3(
        &account.cloud_region,
        &bucket,
        id_token,
        &account.cloud_account_id,
        role,
    )
    .await;

    Ok(Arc::new(NessieCatalog::new(configuration, object_store?)))
}
