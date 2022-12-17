use std::{error::Error, thread::JoinHandle};
use std::env;
use std::sync::{mpsc, Arc, Mutex};
use std::{thread, time};

use tokio::net::TcpListener;

use reqwest::Client;
use reqwest::header::CONTENT_LENGTH;

use http_request::{HttpRequest, HttpResponse};

use serde::Serialize;

#[macro_use] extern crate log;

#[derive(Debug, Serialize)]
enum Status {
    IDLE,
    RUNNING,
}

struct Worker {
    client: Client,
    url: String,
    master_url: String,
    thread: JoinHandle<()>,
    sender: mpsc::Sender<String>,
    status: Arc<Mutex<Status>>,
}

impl Worker {
    fn new(client: Client, url: String, master_url: String) -> Worker {
        let (sender, receiver) = mpsc::channel();
        let master_url_clone = master_url.clone();
        let status = Arc::new(Mutex::new(Status::IDLE));
        let t_status = status.clone();

        let thread = thread::spawn(move || {
            loop {
                let client = reqwest::blocking::Client::new();
                let task = receiver.recv().unwrap();
                thread::sleep(time::Duration::from_secs(15));
                info!("Task {task}");
                info!("Jobs done!");
                let response = "some results".to_string();
                let _ = client.post(format!("{}/result", &master_url_clone))
                    .header(CONTENT_LENGTH, response.len())
                    .body(response)
                    .send();
                let mut status = t_status.lock().unwrap();
                *status = Status::IDLE;
            }
        });

        Worker {
            client,
            url,
            master_url,
            thread,
            sender,
            status,
        }
    }

    async fn register(&self) -> Result<(), Box<dyn Error>> {
        self.client.post(format!("{}/register", self.master_url))
            .header(CONTENT_LENGTH, self.url.len())
            .body(self.url.clone()).send().await?;
        Ok(())
    }

    fn healthcheck(&self) -> Result<HttpResponse, Box<dyn Error>> {
        let status = self.status.lock().unwrap();
        let status = serde_json::to_string(&*status).unwrap();
        let mut res = HttpResponse::new(200, "OK".to_string(), status);
        res.header("Content-Type".to_string(), "application/json".to_string());
        Ok(res)
    }

    fn process_task(&mut self, task: &String) -> Result<HttpResponse, Box<dyn Error>> {
        info!("Processing...");
        let mut status = self.status.lock().unwrap();
        *status = Status::RUNNING;
        self.sender.send(task.to_string()).unwrap();
        Ok(HttpResponse::new(200, "OK".to_string(), "OK".to_string()))
    }
}

async fn route(node: &mut Worker, req: &HttpRequest) -> Result<HttpResponse, Box<dyn Error>> {
    match req.uri.as_deref() {
        Some("/healthcheck") => node.healthcheck(),
        Some("/task") => node.process_task(req.body.as_ref().unwrap()),
        _ => Ok(HttpResponse::new(
            404,
            "NOT FOUND".to_string(),
            "".to_string()
        ))
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
                        let response = route(&mut worker, &req).await;
                        match response {
                            Ok(response) => response.encode(write_stream).await,
                            Err(e) => eprintln!("Something goes wrong {e}")
                        }
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
