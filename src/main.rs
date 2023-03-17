use futures::{StreamExt, TryStreamExt};
use std::error::Error;
use tokio::fs::File;
use reqwest::{
    header::{self, HeaderMap},
    Client,
};

// Config everything here
const AUTHORIZATION: &str = "";
const ENDPOINT: &str = "https://httpbin.org/anything/:id/delete";
const CONCURRENCY: usize = 40;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Open CSV file using a buffered async reader
    let file = File::open("users.csv").await?;
    let mut csv = csv_async::AsyncReader::from_reader(file);

    // HTTP client config
    let mut headers = HeaderMap::new();
    headers.insert(header::AUTHORIZATION, AUTHORIZATION.parse().unwrap());
    headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

    let client = Client::builder().default_headers(headers).build()?;

    // Stream CSV lines directly from the disk and convert
    // them on the fly to HTTP requests, with backpressure
    let count = csv
        .records()
        .inspect_err(|err| eprintln!("Failed to parse CSV: {err}"))
        .filter_map(|result| async { result.ok() })
        .filter_map(|record| async move { record.get(0).map(|id| id.to_string()) })
        .map(|id| ENDPOINT.replace(":id", &id.trim()))
        .inspect(|url| println!("starting {url}"))
        .map(|url| client.delete(url).send())
        .buffer_unordered(CONCURRENCY)
        .inspect_ok(|res| println!("OK {}", res.url()))
        .inspect_err(|err| eprintln!("Error {} : {}", err.url().unwrap(), err))
        .count()
        .await;

    println!("Finished processing {count} items");

    Ok(())
}
