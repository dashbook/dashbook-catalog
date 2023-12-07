use std::sync::Arc;

pub mod aws;

use async_trait::async_trait;
use iceberg_catalog_nessie::{
    apis::configuration::Configuration,
    catalog::{NessieCatalog, ObjectStoreBuilder},
};
use iceberg_rust::catalog::{Catalog, CatalogList};
use postgrest::Postgrest;
use serde::Deserialize;

use crate::postgrest::{get_account, get_catalog_role, get_organization, POSTGREST_URL};

static NESSIE_URL: &str = "https://api.dashbook.dev/nessie";

#[derive(Debug)]
pub struct DashbookS3CatalogList {
    access_token: String,
    id_token: String,
}

impl DashbookS3CatalogList {
    pub fn new(access_token: &str, id_token: &str) -> Self {
        Self {
            access_token: access_token.to_owned(),
            id_token: id_token.to_owned(),
        }
    }
}

#[async_trait]
impl CatalogList for DashbookS3CatalogList {
    async fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        let organization = get_organization(&self.access_token).ok()?;

        let mut configuration = Configuration::new();
        configuration.base_path = NESSIE_URL.to_owned() + "/" + &organization;
        configuration.bearer_access_token = Some(self.access_token.clone());

        let account = get_account(&self.access_token, name).await.ok()?;

        let role = get_catalog_role(&self.access_token, name, "write")
            .await
            .ok()??;

        // #[cfg(feature = "aws")]
        let builder = aws::get_s3(
            &account.cloud_region,
            &self.id_token,
            &account.cloud_account_id,
            &role,
        )
        .await
        .ok()?;

        Some(Arc::new(NessieCatalog::new(
            configuration,
            ObjectStoreBuilder::S3(builder),
        )))
    }
    async fn list_catalogs(&self) -> Vec<String> {
        let postgrest = Postgrest::new(POSTGREST_URL)
            .insert_header("Authorization", "Bearer ".to_string() + &self.access_token);

        let organization = get_organization(&self.access_token).unwrap();

        postgrest
            .from("catalog")
            .select("catalog_name")
            .eq("organization_id", organization)
            .execute()
            .await
            .unwrap()
            .json::<Vec<CatalogResponse>>()
            .await
            .unwrap()
            .into_iter()
            .map(|x| x.catalog_name)
            .collect()
    }
}

#[derive(Deserialize)]
pub struct CatalogResponse {
    catalog_name: String,
}
