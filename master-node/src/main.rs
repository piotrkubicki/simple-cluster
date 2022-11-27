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

type HealthCheckRes = Box<dyn Future<Output=()> + Send + 'static>;

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
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(6));
            loop {
                info!("Checkings workers health...");
                interval.tick().await;
                let workers = &mut workers.lock().await;
                info!("Workers: {}", workers.len());
            //        for worker in workers {
                if workers.len() > 0 {
                    workers[0].healthcheck(&client).await;
                }
            }
        });
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
        }
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
    tokio::join!(async {loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let (read_stream, write_stream) = stream.into_split();
                let req = HttpRequest::decode(read_stream).await;
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
                    Err(e) => eprintln!("{e}")
                }
            },
            _ => continue,
        }
    }});

    Ok(())
}
