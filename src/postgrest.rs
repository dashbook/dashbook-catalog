use postgrest::Postgrest;
use serde::Deserialize;

use crate::error::Error;

static POSTGREST_URL: &str = "https://api.dashbook.dev/rest/v1";
static POSTGREST_ORG_URL: &str = "https://org.dashbook.dev/rest/v1";

#[derive(Deserialize)]
pub(crate) struct Role {
    pub role_id: String,
}

#[derive(Deserialize)]
pub(crate) struct Table {
    pub metadata_location: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Account {
    pub cloud_account_id: String,
    pub cloud_region: String,
}

pub(crate) async fn get_account(access_token: &str, catalog_name: &str) -> Result<Account, Error> {
    let postgrest = Postgrest::new(POSTGREST_ORG_URL)
        .insert_header("Authorization", "Bearer ".to_string() + access_token);

    let mut account: Vec<Account> = postgrest
        .from("catalogs")
        .select("cloud_account_id,cloud_region")
        .eq("catalog_name", catalog_name)
        .limit(1)
        .execute()
        .await?
        .json()
        .await?;

    account
        .pop()
        .ok_or(Error::EmptyResponse("account".to_string()))
}

pub async fn get_role(
    access_token: &str,
    catalog_name: &str,
    table_namespace: &str,
    table_name: &str,
    permission: &str,
) -> Result<String, Error> {
    let role =
        match get_namespace_role(access_token, catalog_name, &table_namespace, permission).await? {
            None => get_table_role(
                access_token,
                catalog_name,
                table_namespace,
                table_name,
                permission,
            )
            .await?
            .ok_or(Error::NoPermission(table_name.to_string()))?,
            Some(role) => role,
        };
    Ok(role)
}

pub(crate) async fn get_namespace_role(
    access_token: &str,
    catalog_name: &str,
    table_namespace: &str,
    permission: &str,
) -> Result<Option<String>, Error> {
    let postgrest = Postgrest::new(POSTGREST_URL)
        .insert_header("Authorization", "Bearer ".to_string() + access_token);

    let mut role: Vec<Role> = postgrest
        .from("namespace_permission")
        .select("role_id")
        .eq("catalog_name", catalog_name)
        .eq("table_namespace", table_namespace)
        .eq("permissions->>".to_string() + permission, "true")
        .limit(1)
        .execute()
        .await?
        .json()
        .await?;

    Ok(role.pop().map(|role| role.role_id))
}

pub(crate) async fn get_table_role(
    access_token: &str,
    catalog_name: &str,
    table_namespace: &str,
    table_name: &str,
    permission: &str,
) -> Result<Option<String>, Error> {
    let postgrest = Postgrest::new(POSTGREST_URL)
        .insert_header("Authorization", "Bearer ".to_string() + access_token);

    let mut role: Vec<Role> = postgrest
        .from("resource_permission")
        .select("role_id")
        .eq("catalog_name", catalog_name)
        .eq("table_namespace", table_namespace)
        .eq("table_name", table_name)
        .eq("permissions->>".to_string() + permission, "true")
        .limit(1)
        .execute()
        .await?
        .json()
        .await?;

    Ok(role.pop().map(|role| role.role_id))
}

pub(crate) async fn get_bucket(
    access_token: &str,
    catalog_name: &str,
    table_namespace: &str,
    table_name: &str,
) -> Result<String, Error> {
    let postgrest = Postgrest::new(POSTGREST_URL)
        .insert_header("Authorization", "Bearer ".to_string() + access_token);

    let mut table: Vec<Table> = postgrest
        .from("iceberg_tables")
        .select("metadata_location")
        .eq("catalog_name", catalog_name)
        .eq("table_namespace", table_namespace)
        .eq("table_name", table_name)
        .eq("branch", "main")
        .limit(1)
        .execute()
        .await?
        .json()
        .await?;

    let metadata_location = table
        .pop()
        .ok_or(Error::EmptyResponse("iceberg tables".to_string()))?
        .metadata_location;

    let bucket = metadata_location
        .trim_start_matches("s3://")
        .split("/")
        .next()
        .ok_or(Error::Other("metadata location is empty".to_string()))?;

    Ok(bucket.to_string())
}
