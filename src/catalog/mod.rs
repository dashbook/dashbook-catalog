use std::{collections::HashMap, sync::Arc};

pub mod aws;

use async_trait::async_trait;
use iceberg_catalog_nessie::{apis::configuration::Configuration, catalog::NessieCatalog};
use iceberg_rust::catalog::{bucket::ObjectStoreBuilder, Catalog, CatalogList};
use postgrest::Postgrest;
use serde::Deserialize;

use crate::{
    error::Error,
    postgrest::{get_accounts, get_catalog_roles, get_organization, POSTGREST_URL},
};

static NESSIE_URL: &str = "https://api.dashbook.dev/nessie";

#[derive(Debug)]
struct Role {
    cloud_region: String,
    cloud_account_id: String,
    role_id: String,
}

#[derive(Debug)]
pub struct DashbookS3CatalogList {
    access_token: String,
    id_token: String,
    roles: HashMap<String, Role>,
}

impl DashbookS3CatalogList {
    pub async fn new(access_token: &str, id_token: &str) -> Result<Self, Error> {
        let accounts = get_accounts(access_token).await?;
        let role_ids = get_catalog_roles(access_token, "write").await?;

        let roles = HashMap::from_iter(accounts.into_iter().map(|x| {
            let role = Role {
                cloud_region: x.cloud_region,
                cloud_account_id: x.cloud_account_id,
                role_id: role_ids.get(&x.catalog_name).unwrap().clone(),
            };
            (x.catalog_name, role)
        }));

        Ok(Self {
            access_token: access_token.to_owned(),
            id_token: id_token.to_owned(),
            roles,
        })
    }
}

#[async_trait]
impl CatalogList for DashbookS3CatalogList {
    async fn catalog(&self, name: &str) -> Option<Arc<dyn Catalog>> {
        let organization = get_organization(&self.access_token).ok()?;

        let mut configuration = Configuration::new();
        configuration.base_path = NESSIE_URL.to_owned() + "/" + &organization;
        configuration.bearer_access_token = Some(self.access_token.clone());

        let role = self.roles.get(name)?;

        // #[cfg(feature = "aws")]
        let builder = aws::get_s3(
            &role.cloud_region,
            &self.id_token,
            &role.cloud_account_id,
            &role.role_id,
        )
        .await
        .ok()?;

        Some(Arc::new(NessieCatalog::new_with_hash(
            configuration,
            ObjectStoreBuilder::S3(builder),
            None,
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
