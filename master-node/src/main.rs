#[macro_use] extern crate log;

use core::fmt;
use std::error::Error;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::task;
use tokio::sync::Mutex;
use http_request::{HttpRequest, HttpResponse};

use futures::future::join_all;

use reqwest::Client;

use serde::Deserialize;

const IP_ADDRES: &str = "0.0.0.0";
const PORT_NUMBER: isize = 5100;

#[derive(Debug, Deserialize)]
enum WorkerStatus {
    IDLE,
    RUNNING,
    ERROR,
}

#[derive(Debug, Clone)]
struct WorkerConnectionError {}

impl Error for WorkerConnectionError{}

struct Worker {
    url: String,
    handle: Option<JoinHandle<String>>,
    status: WorkerStatus,
}

impl Worker {
    fn new(url: String) -> Worker {
        let handle = None;
        let status = WorkerStatus::IDLE;
        Worker {
            url,
            handle,
            status,
        }
    }

    async fn healthcheck(&mut self, client: &Client) {
        let healthcheck = client.get(format!("{}/healthcheck", self.url)).send();
        match tokio::time::timeout(tokio::time::Duration::from_secs(2), healthcheck).await {
            Ok(res) => {
                match res {
                    Ok(res) => {
                        if !res.status().is_success() {
                            self.status = WorkerStatus::ERROR;
                        } else {
                            match res.json().await {
                                Ok(status) => self.status = status,
                                Err(e) => eprintln!("{e}"),
                            }
                        }
                    },
                    Err(_) => self.status = WorkerStatus::ERROR,
                }
            },
            Err(e) => eprintln!("Something goes wrong {e}"),
        }
    }
}

struct MasterNode {
    workers: Arc<Mutex<Vec<Worker>>>,
    client: Client,
}

impl MasterNode {
    fn new() -> MasterNode {
        let workers = Arc::new(Mutex::new(Vec::new()));
        let client = Client::new();
        MasterNode {
            workers,
            client,
        }
    }

    async fn register_worker(&mut self, worker_url: String) -> Result<HttpResponse, Box<dyn Error>> {
        let worker = Worker::new(worker_url);
        self.workers.lock().await.push(worker);
        Ok(HttpResponse::new(
            200,
            "OK".to_string(),
            "OK".to_string(),
        ))
    }

    async fn check_workers_status(&self) -> Result<HttpResponse, Box<dyn Error>> {
        let workers = &self.workers.lock().await;
        let mut response = String::new();
        for worker in workers.iter() {
            response.push_str(&format!("URL: {} STATUS: {:?}\n", worker.url, worker.status));
        }
        Ok(HttpResponse::new(
            200,
            "OK".to_string(),
            response,
        ))
    }

    fn workers_healthcheck(&mut self) {
        let workers = self.workers.clone();
        let client = self.client.clone();
        task::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                info!("Checkings workers health...");
                let mut workers = workers.lock().await;
                let tasks: Vec<_> = workers.iter_mut().map(|worker| worker.healthcheck(&client)).collect();
                join_all(tasks).await;
                info!("Health check done");
            }
        });
    }

    async fn run_task(&self, req: &HttpRequest) -> Result<HttpResponse, Box<dyn Error>> {
        let workers = self.workers.clone();
        let client = self.client.clone();

        match &req.body {
            Some(task) => {
                let task = task.clone();
                tokio::task::spawn(async move {
                    info!("Running task...");
                    let mut workers = workers.as_ref().lock().await;
                    for worker in &mut *workers {
                        let _ = client.post(format!("{}/task", worker.url))
                            .header("content-length", task.len())
                            .body(task.to_string()).send().await;
                        worker.status = WorkerStatus::RUNNING;
                    }
                }).await?;
            },
            None => info!("No data received for processing"),
        }

        Ok(HttpResponse::new(
            200,
            "OK".to_string(),
            "OK".to_string(),
        ))
    }

    async fn process_result(&self, _req: &HttpRequest) -> Result<HttpResponse, Box<dyn Error>> {
        Ok(HttpResponse::new(
            200,
            "OK".to_string(),
            "OK".to_string(),
        ))
    }
}

async fn route(node: &mut MasterNode, req: &HttpRequest) -> Result<HttpResponse, Box<dyn Error>> {
    match req.uri.as_deref() {
        Some("/register") => {
            info!("Registering");
            let worker_url = &req.body.as_ref().unwrap();
            node.register_worker(worker_url.to_string()).await
        },
        Some("/status") => node.check_workers_status().await,
        Some("/task") => node.run_task(req).await,
        Some("/result") => node.process_result(req).await,
        _ => Ok(HttpResponse::new(404, "NOT FOUND".to_string(), "".to_string())),
    }
}

impl fmt::Display for WorkerConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "cannot connect with worker")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let mut node = MasterNode::new();
    let addr = format!("{}:{}", IP_ADDRES, PORT_NUMBER);
    let listener = TcpListener::bind(&addr).await?;

    node.workers_healthcheck();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let (read_stream, write_stream) = stream.into_split();
                let req = HttpRequest::decode(read_stream).await;
                match req {
                    Ok(req) => {
                        let response = route(&mut node, &req).await;
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
