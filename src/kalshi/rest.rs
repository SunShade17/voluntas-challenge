//! Market discovery over the public REST API.

use std::time::Duration;

use anyhow::bail;
use reqwest::{StatusCode, Url};
use serde::Deserialize;
use serde::de::DeserializeOwned;

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct Market {
    ticker: String,
    #[serde(default)]
    mve_collection_ticker: Option<String>,
}

/// Tickers of every open market, excluding multivariate (MVE) markets.
pub async fn open_markets(http: &reqwest::Client, rest_url: &str) -> anyhow::Result<Vec<String>> {
    let mut tickers = Vec::new();
    let mut cursor = String::new();
    loop {
        let mut params = vec![("status", "open"), ("limit", "1000"), ("mve_filter", "exclude")];
        if !cursor.is_empty() {
            params.push(("cursor", &cursor));
        }
        let url = Url::parse_with_params(&format!("{rest_url}/markets"), &params)?;
        let page: MarketsPage = get_json(http, url).await?;

        // `mve_filter` should already do this; the check is cheap insurance.
        tickers.extend(
            page.markets
                .into_iter()
                .filter(|m| m.mve_collection_ticker.is_none())
                .map(|m| m.ticker),
        );
        match page.cursor {
            Some(next) if !next.is_empty() => cursor = next,
            _ => return Ok(tickers),
        }
    }
}

async fn get_json<T: DeserializeOwned>(http: &reqwest::Client, url: Url) -> anyhow::Result<T> {
    for attempt in 0..6 {
        let response = http.get(url.clone()).send().await?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            tokio::time::sleep(Duration::from_millis(250 << attempt)).await;
            continue;
        }
        return Ok(response.error_for_status()?.json().await?);
    }
    bail!("still rate limited after retries: {url}")
}
