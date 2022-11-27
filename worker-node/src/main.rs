use std::error::Error;
use std::env;
use std::str;

use std::io::BufRead;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, Duration};

use reqwest::Client;
use reqwest::header::CONTENT_LENGTH;

#[macro_use] extern crate log;


struct Worker {
    client: Client,
    url: String,
    master_url: String,
}

impl Worker {
    fn new(client: Client, url: String, master_url: String) -> Worker {
        Worker {
            client,
            url,
            master_url,
        }
    }

    async fn register(&self) -> Result<(), Box<dyn Error>> {
        self.client.post(format!("{}/register", self.master_url))
            .header(CONTENT_LENGTH, self.url.len())
            .body(self.url.clone()).send().await?;
        Ok(())
    }
}

async fn process_request(node: &mut Worker, mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut message: Vec<u8> = Vec::new();
    loop {
        let mut buffer = [0; 1024];

        match stream.read(&mut buffer).await {
            Ok(n) if n == 0 => {
                info!("Finished reading data!");
                break;
            },
            Ok(n) => {
                let buf = &buffer[..n];
                message.extend_from_slice(buf);
                let request: Vec<_> = str::from_utf8(&message).unwrap().rsplit("\r\n\r\n").collect();
                if request.len() >= 2 {
                    info!("{:?}", request);
                    let body = request[0];
                    let headers = request[1];

                    let content: Vec<_> = headers.lines().filter(|line| line.to_lowercase().starts_with("content-length: ")).collect();

                    if content.len() > 0 {
                        let content_length: Vec<_> = content[0].split(": ").collect();
                        let content_length: usize = content_length[1].parse().unwrap();

                        if body.len() >= content_length {
                            let request: Vec<_> = message.lines().map(|line| line.unwrap()).filter(|line| !line.is_empty()).collect();
                            route(node, request, &mut stream).await;
                            break;
                        }
                    } else {
                        let request: Vec<_> = message.lines().map(|line| line.unwrap()).filter(|line| !line.is_empty()).collect();
                        route(node, request, &mut stream).await;
                        break;
                    }
                }
            },
            Err(e) => {
                eprintln!("Error: {}", e);
                break;
            }
        };
    }
    let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\nGit", 3);
    if let Err(e) = stream.write_all(&response.as_bytes()).await {
        eprintln!("Error writing back: {}", e);
    }
    Ok(())
}

async fn route(node: &mut Worker, request: Vec<String>, stream: &mut TcpStream) {
    info!("Processing route");
    info!("{:?}", request);
    match request[0].as_str() {
        "GET /healthcheck HTTP/1.1" => {
            let _ = stream.write_all("HTTP/1.1 200 OK".as_bytes()).await;
        },
        _ => println!("404"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let ip_addr = env::var("IP_ADDRES").unwrap_or("0.0.0.0".to_string());
    let port = env::var("PORT_NUMBER").unwrap_or("5101".to_string());

    let addr = format!("{}:{}", ip_addr, port);

    let client = Client::new();
    let worker_url = env::var("WORKER_URL").unwrap();
    let master_url = env::var("MASTER_URL").unwrap();
    let mut worker = Worker::new(client, worker_url, master_url);
    info!("Registering...");
    worker.register().await?;
    info!("Request accepted. Listening...");

    let addr = format!("{}:{}", ip_addr, port);
    let listener = TcpListener::bind(&addr).await?;
    loop {
        info!("Listening...");
        let (stream, _) = listener.accept().await?;
        let _ = process_request(&mut worker, stream).await;
    }
}
