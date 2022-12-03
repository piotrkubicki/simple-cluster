#[macro_use] extern crate log;

use core::fmt;
use std::error::Error;
use std::io::BufRead;
use std::sync::Arc;
use std::future::Future;

use tokio::net::{TcpListener, TcpStream};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use tokio::task;
use tokio::time::{sleep, Duration};
use tokio::sync::Mutex;
use http_request::{HttpRequest, HttpResponse};

use reqwest::Client;

const IP_ADDRES: &str = "0.0.0.0";
const PORT_NUMBER: isize = 5100;

#[derive(Debug)]
enum WorkerStatus {
    IDLE,
    RUNNING,
    ERROR,
}

#[derive(Debug, Clone)]
struct WorkerConnectionError {
    id: usize
}

impl Error for WorkerConnectionError{}

impl WorkerConnectionError {
    fn new(id: usize) -> WorkerConnectionError {
        WorkerConnectionError { id }
    }
}


struct Worker {
    id: usize,
    url: String,
    handle: Option<JoinHandle<String>>,
    status: WorkerStatus,
}

impl Worker {
    fn new(id: usize, url: String) -> Worker {
        let handle = None;
        let status = WorkerStatus::IDLE;
        Worker {
            id,
            url,
            handle,
            status,
        }
    }

    async fn healthcheck(&mut self, client: &Client) {
        info!("Checking");
        if let Ok(res) = client.get(format!("{}/healthcheck", self.url)).send().await {
            info!("{:?}", res);
            if !res.status().is_success() {
                self.status = WorkerStatus::ERROR;
            }
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

    async fn register_worker(&mut self, worker_url: String) {
        let worker = Worker::new(1, worker_url);
        self.workers.lock().await.push(worker);
    }

    async fn check_workers_status(&self) {
        let workers = &self.workers.lock().await;
        for worker in workers.iter() {
            info!("URL: {} STATUS: {:?}", worker.url, worker.status);
        }
    }

    fn workers_healthcheck(&mut self) {
        let workers = self.workers.clone();
        let client = self.client.clone();
        task::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                info!("Checkings workers health...");
                interval.tick().await;
                let mut workers = workers.lock().await;
                for worker in &mut *workers {
                    worker.healthcheck(&client).await;
                }
            }
        });
    }

    async fn run_task(&self, http_request: &HttpRequest) -> Result<(), Box<dyn Error>>{
        let workers = self.workers.clone();
        let client = self.client.clone();
        let join = tokio::task::spawn(async move {
            info!("Running task...");
            let mut workers = workers.as_ref().lock().await;
            for worker in &mut *workers {
                client.post(format!("{}/task", worker.url))
                    .header("content-length", 3)
                    .body("aaa").send().await;
            }

        });

        join.await?;
        Ok(())
    }
}

async fn route(node: &mut MasterNode, request: &HttpRequest) {
    match request.uri.as_deref() {
        Some("/register") => {
            println!("Registering");
            let worker_url = &request.body.as_ref().unwrap();
            node.register_worker(worker_url.to_string()).await;
        },
        Some("/status") => {
            println!("Checking status");
            node.check_workers_status().await;
        },
        Some("/task") => {
            let _ = node.run_task(&request).await;
        },
        _ => println!("404"),
    }
}

impl fmt::Display for WorkerConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "cannot connect with worker {}", self.id)
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
                info!("{:?}", req);
                match req {
                    Ok(req) => {
                        route(&mut node, &req).await;
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
