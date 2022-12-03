use std::{error::Error, thread::JoinHandle};
use std::env;
use std::sync::mpsc;
use std::{thread, time};

use tokio::net::TcpListener;

use reqwest::Client;
use reqwest::header::CONTENT_LENGTH;

use http_request::{HttpRequest, HttpResponse};

#[macro_use] extern crate log;

struct Worker {
    client: Client,
    url: String,
    master_url: String,
    thread: JoinHandle<()>,
    sender: mpsc::Sender<usize>
}

impl Worker {
    fn new(client: Client, url: String, master_url: String) -> Worker {
        let (sender, receiver) = mpsc::channel();
        let thread = thread::spawn(move || {
            loop {
                let _ = receiver.recv().unwrap();
                thread::sleep(time::Duration::from_secs(5));
                info!("Jobs done!");
            }
        });

        Worker {
            client,
            url,
            master_url,
            thread,
            sender,
        }
    }

    async fn register(&self) -> Result<(), Box<dyn Error>> {
        self.client.post(format!("{}/register", self.master_url))
            .header(CONTENT_LENGTH, self.url.len())
            .body(self.url.clone()).send().await?;
        Ok(())
    }

    fn healthcheck(&self) {
        info!("Worker alive");
    }

    fn process_task(&self) {
        info!("Processing...");
        self.sender.send(2).unwrap();
    }
}


async fn route(node: &mut Worker, req: &HttpRequest) {
    match req.uri.as_deref() {
        Some("/healthcheck") => node.healthcheck(),
        Some("/task") => node.process_task(),
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
    let worker_url = format!("http://localhost:{port}");
    let master_url = env::var("MASTER_URL").unwrap();
    let mut worker = Worker::new(client, worker_url, master_url);
    info!("Registering...");
    worker.register().await?;
    info!("Request accepted. Listening...");

    let listener = TcpListener::bind(&addr).await?;
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let (read_stream, write_stream) = stream.into_split();
                let req = HttpRequest::decode(read_stream).await;
                match req {
                    Ok(req) => {
                        route(&mut worker, &req).await;
                        let response = HttpResponse::new(
                            req.version.unwrap(),
                            200,
                            "OK".to_string(),
                            "OK".to_string(),
                        );
                        response.encode(write_stream).await;
                    },
                    Err(e) => {
                        eprintln!("{e}");
                        break;
                    }
                }
            },
            Err(e) => {
                eprintln!("{e}");
                break;
            }
        }
    }

    Ok(())
}
