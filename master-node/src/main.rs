#[macro_use] extern crate log;

use core::fmt;
use std::error::Error;
use std::sync::Arc;
use std::collections::HashMap;

use tokio::net::TcpListener;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::task;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use http_request::{HttpRequest, HttpResponse};

use futures::future::join_all;

use reqwest::Client;

use serde::Deserialize;

use aws_sdk_s3 as s3;

const IP_ADDRES: &str = "0.0.0.0";
const PORT_NUMBER: isize = 5100;

#[derive(Debug, Deserialize, PartialEq)]
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
    status: WorkerStatus,
}

impl Worker {
    fn new(url: String) -> Worker {
        let status = WorkerStatus::IDLE;
        Worker {
            url,
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

    fn workers_healthcheck(&mut self) {
        let workers = self.workers.clone();
        let client = self.client.clone();
        task::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                info!("Checkings workers health...");
                let mut workers = workers.lock().await;
                let tasks: Vec<_> = workers.iter_mut().map(|worker| worker.healthcheck(&client)).collect();
                join_all(tasks).await;

                let mut failed_workers: Vec<usize> = Vec::new();
                for (i, worker) in workers.iter().enumerate() {
                    if worker.status == WorkerStatus::ERROR {
                        failed_workers.push(i);
                    }
                }
                for failed_worker in failed_workers {
                    workers.remove(failed_worker);
                }
                info!("Health check done");
            }
        });
    }
}

async fn get_idle_worker_index(workers: &Mutex<Vec<Worker>>) -> usize {
    loop {
        sleep(Duration::from_secs(3)).await;
        let workers = workers.lock().await;
        for (i, worker) in workers.iter().enumerate() {
            if worker.status == WorkerStatus::IDLE {
                return i;
            }
        }
        info!("Waiting...");
    }
}

fn register_worker(req: HttpRequest, write_stream: OwnedWriteHalf, node: &MasterNode) -> Result<(), Box<dyn Error>> {
    let workers = node.workers.clone();
    task::spawn(async move {
        let worker_url = req.body.as_ref().unwrap();
        let worker = Worker::new(worker_url.to_string());
        workers.lock().await.push(worker);
        let res = HttpResponse::new(
            200,
            "OK".to_string(),
            "OK".to_string(),
        );
        res.encode(write_stream).await;
    });

    Ok(())
}

fn check_workers_status(_req: HttpRequest, write_stream: OwnedWriteHalf, node: &MasterNode) -> Result<(), Box<dyn Error>> {
    let workers = node.workers.clone();
    task::spawn(async move {
        let workers = workers.lock().await;
        let mut response = String::new();
        for worker in workers.iter() {
            response.push_str(&format!("URL: {} STATUS: {:?}\n", worker.url, worker.status));
        }
        let res = HttpResponse::new(
            200,
            "OK".to_string(),
            response,
        );
        res.encode(write_stream).await;
    });

    Ok(())
}

fn run_task(req: HttpRequest, write_stream: OwnedWriteHalf, node: &MasterNode) -> Result<(), Box<dyn Error>> {
    let workers = node.workers.clone();
    let client = node.client.clone();

    match &req.body {
        Some(task) => {
            let bucket = task.clone();
            task::spawn(async move {
                info!("Running task...");
                let config = aws_config::load_from_env().await;
                let s3_client = s3::Client::new(&config);
                let res = s3_client.list_objects_v2().bucket(&bucket).send().await;

                match res {
                    Ok(res) => {
                        for obj in res.contents().unwrap_or_default() {
                            let file = obj.key().unwrap_or_default().to_string();
                            info!("File {file}....");
                            let path = format!("{bucket}/{file}");
                            let worker_index = get_idle_worker_index(workers.as_ref()).await;
                            let mut workers = workers.as_ref().lock().await;
                            let _ = client.post(format!("{}/task", workers[worker_index].url))
                                .header("content-length", path.len())
                                .body(path).send().await;
                            workers[worker_index].status = WorkerStatus::RUNNING;
                        }
                    },
                    Err(e) => println!("Something goes wrong! {}", e),
                }
            });
            task::spawn(async move {
                let res = HttpResponse::new(
                    200,
                    "OK".to_string(),
                    "OK".to_string(),
                );
                res.encode(write_stream).await;
            });
        },
        None => info!("No data received for processing"),
    }

    Ok(())
}

fn process_result(_req: HttpRequest, write_stream: OwnedWriteHalf, node: &MasterNode) -> Result<(), Box<dyn Error>> {
    task::spawn(async move{
        let res = HttpResponse::new(
            200,
            "OK".to_string(),
            "OK".to_string(),
        );
        res.encode(write_stream).await;
    });

    Ok(())
}

struct Router {
    routes: HashMap<String, fn(HttpRequest, OwnedWriteHalf, &MasterNode) -> Result<(), Box<dyn Error>>>,
}

impl Router {
    fn new() -> Self {
        Router {
            routes: HashMap::new(),
        }
    }

    fn register(&mut self, url: String, action: fn(HttpRequest, OwnedWriteHalf, &MasterNode) -> Result<(), Box<dyn Error>>) {
        self.routes.insert(url, action);
    }

    async fn process(&self, stream: tokio::net::TcpStream, node: &MasterNode) {
        let (read_stream, write_stream) = stream.into_split();
        let req = HttpRequest::decode(read_stream).await;
        let uri = req.as_ref().unwrap().uri.as_ref();
        let route = self.routes.get(uri.unwrap());
        match route {
            Some(route) => (route)(req.unwrap(), write_stream, &node),
            None => {
                let res = HttpResponse::new(404, "NOT_FOUND".to_string(), "NOT_FOUND".to_string());
                res.encode(write_stream).await;
                Ok(())
            },
        };
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
    let mut router = Router::new();
    router.register("/register".to_string(), register_worker);
    router.register("/status".to_string(), check_workers_status);
    router.register("/task".to_string(), run_task);
    router.register("/result".to_string(), process_result);

    let addr = format!("{}:{}", IP_ADDRES, PORT_NUMBER);
    let listener = TcpListener::bind(&addr).await?;

    node.workers_healthcheck();

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                router.process(stream, &node).await;
            },
            Err(e) => {
                eprintln!("{e}");
                break;
            }
        }
    }

    Ok(())
}
