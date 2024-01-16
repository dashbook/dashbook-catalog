use std::collections::HashMap;

use postgrest::Postgrest;
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::error::Error;

pub(crate) static POSTGREST_URL: &str = "https://api.dashbook.dev/rest/v1";

#[derive(Deserialize)]
pub(crate) struct Role {
    pub catalog_name: String,
    pub role_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Account {
    pub catalog_name: String,
    pub cloud_account_id: String,
    pub cloud_region: String,
}

pub(crate) async fn get_accounts(access_token: &str) -> Result<Vec<Account>, Error> {
    let postgrest = Postgrest::new(POSTGREST_URL)
        .insert_header("Authorization", "Bearer ".to_string() + access_token);

    let organization = get_organization(access_token)?;

    let accounts: Vec<Account> = postgrest
        .from("catalog")
        .select("catalog_name, cloud_account_id, cloud_region")
        .eq("organization_id", organization)
        .execute()
        .await?
        .json()
        .await?;

    Ok(accounts)
}

pub(crate) async fn get_catalog_roles(
    access_token: &str,
    permission: &str,
) -> Result<HashMap<String, String>, Error> {
    let postgrest = Postgrest::new(POSTGREST_URL)
        .insert_header("Authorization", "Bearer ".to_string() + access_token);

    let organization = get_organization(access_token)?;

    let roles: Vec<Role> = postgrest
        .from("catalog_permission")
        .select("catalog_name, role_id")
        .eq("organization_id", organization)
        .eq("permissions->>".to_string() + permission, "true")
        .execute()
        .await?
        .json()
        .await?;

    Ok(roles.into_iter().fold(HashMap::new(), |mut acc, x| {
        acc.entry(x.catalog_name).or_insert(x.role_id);
        acc
    }))
}

pub async fn get_role(
    access_token: &str,
    catalog_name: &str,
    table_namespace: &str,
    table_name: &str,
    permission: &str,
) -> Result<String, Error> {
    let role = match get_catalog_role(access_token, catalog_name, permission).await? {
        None => match get_namespace_role(access_token, catalog_name, table_namespace, permission)
            .await?
        {
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
        },
        Some(role) => role,
    };
    Ok(role)
}

pub(crate) async fn get_catalog_role(
    access_token: &str,
    catalog_name: &str,
    permission: &str,
) -> Result<Option<String>, Error> {
    let postgrest = Postgrest::new(POSTGREST_URL)
        .insert_header("Authorization", "Bearer ".to_string() + access_token);

    let organization = get_organization(access_token)?;

    let mut role: Vec<Role> = postgrest
        .from("catalog_permission")
        .select("role_id")
        .eq("organization_id", organization)
        .eq("catalog_name", catalog_name)
        .eq("permissions->>".to_string() + permission, "true")
        .limit(1)
        .execute()
        .await?
        .json()
        .await?;

    Ok(role.pop().map(|role| role.role_id))
}

pub(crate) async fn get_namespace_role(
    access_token: &str,
    catalog_name: &str,
    table_namespace: &str,
    permission: &str,
) -> Result<Option<String>, Error> {
    let postgrest = Postgrest::new(POSTGREST_URL)
        .insert_header("Authorization", "Bearer ".to_string() + access_token);

    let organization = get_organization(access_token)?;

    let mut role: Vec<Role> = postgrest
        .from("namespace_permission")
        .select("role_id")
        .eq("organization_id", organization)
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

    let organization = get_organization(access_token)?;

    let mut role: Vec<Role> = postgrest
        .from("resource_permission")
        .select("role_id")
        .eq("organization_id", organization)
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

pub(crate) fn get_organization(access_token: &str) -> Result<String, Error> {
    let json = String::from_utf8(base64_url::decode(
        &access_token.split('.').collect::<Vec<_>>()[1],
    )?)?;
    let claims: JsonValue = serde_json::from_str(&json)?;

    if let JsonValue::String(s) = &claims["organization"] {
        Ok(s.to_owned())
    } else {
        Err(Error::Other(
            "Organization claim is not a string".to_string(),
        ))
    }
}
