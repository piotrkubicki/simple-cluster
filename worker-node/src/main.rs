use std::sync::mpsc::Sender;
use std::error::Error;
use std::env;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::str;
use std::collections::HashMap;

use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use tokio::net::TcpListener;
use tokio::task;
use tokio::runtime::Handle;

use reqwest::Client;
use reqwest::header::CONTENT_LENGTH;

use aws_sdk_s3 as s3;

use commons::{Route, Router};

use serde::Serialize;

#[macro_use] extern crate log;

#[derive(Debug, Serialize)]
enum Status {
    IDLE,
    RUNNING,
}

#[derive(Serialize)]
struct TaskResult {
    data: HashMap<String, usize>,
}

impl TaskResult {
    fn new(data: HashMap<String, usize>) -> Self {
        TaskResult {
            data
        }
    }
}

struct Worker {
    client: ClientWithMiddleware,
    url: String,
    master_url: String,
    sender: mpsc::Sender<String>,
    status: Arc<Mutex<Status>>,
}

impl Worker {
    fn new(client: ClientWithMiddleware, url: String, master_url: String) -> Worker {
        let (sender, receiver) = mpsc::channel();
        let master_url_clone = master_url.clone();
        let status = Arc::new(Mutex::new(Status::IDLE));
        let t_status = status.clone();
        let handle = Handle::current();

        thread::spawn(move || {
            loop {
                let client = reqwest::blocking::Client::new();
                let task: String = receiver.recv().unwrap();
                if let Some((bucket, key)) = task.rsplit_once("/") {
                    let result = handle.block_on(async {
                        let mut result = HashMap::new();
                        let config = aws_config::load_from_env().await;
                        let s3_client = s3::Client::new(&config);
                        let res = s3_client.get_object().bucket(bucket).key(key).send().await;
                        if let Ok(res) = res.unwrap().body.collect().await {
                            let data = res.into_bytes();
                            let data = str::from_utf8(&data).unwrap()
                                .split_whitespace()
                                .map(|x| x.to_lowercase().replace(&['(', ')', '.', ','][..], ""));

                            for item in data {
                                *result.entry(item).or_insert(0) += 1;
                            }
                        }
                        result
                    });
                    debug!("Jobs done!");
                    let task_result = TaskResult::new(result);
                    let response = serde_json::to_string(&task_result).unwrap();
                    let _ = client.post(format!("{}/result", &master_url_clone))
                        .header(CONTENT_LENGTH, response.len())
                        .body(response)
                        .send();
                    let mut status = t_status.lock().unwrap();
                    *status = Status::IDLE;
                }
            }
        });

        Worker {
            client,
            url,
            master_url,
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

}

fn healthcheck(status: Arc<Mutex<Status>>) -> Route {
    let status = status.clone();

    Box::new(move |_req, res| {
        let status = status.clone();
        task::spawn(async move {
            let status = {
                let status = status.lock().unwrap();
                serde_json::to_string(&*status).unwrap()
            };
            res.status("OK".to_string())
                .status_code(200)
                .body(status)
                .add_header("Content-Type".to_string(), "application/json".to_string())
                .send().await;
        });

        Ok(())
    })
}

fn process_task(status: Arc<Mutex<Status>>, sender: Sender<String>) -> Route {
    let status = status.clone();

    Box::new(move |req, res| {
        let status = status.clone();
        let sender = sender.clone();
        task::spawn(async move {
            debug!("Processing...");
            {
                let mut status = status.lock().unwrap();
                *status = Status::RUNNING;
            }
            sender.send(req.body.unwrap()).unwrap();
            res.status("OK".to_string())
                .status_code(200)
                .body("ACCEPTED".to_string())
                .send().await
        });

        Ok(())
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let ip_addr = env::var("WORKER_IP").unwrap_or("0.0.0.0".to_string());
    let port = env::var("WORKER_PORT").unwrap_or("5101".to_string());
    let addr = format!("{}:{}", ip_addr, port);

    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(10);
    let client = ClientBuilder::new(Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();
    let worker_url = format!("http://{addr}");
    let master_host = env::var("SIMPLE_CLUSTER_MASTER_NODE_SVC_SERVICE_HOST").unwrap_or("0.0.0.0".to_string());
    let master_port = env::var("SIMPLE_CLUSTER_MASTER_NODE_SVC_SERVICE_PORT").unwrap_or("5100".to_string());
    let master_url = format!("http://{master_host}:{master_port}");
    let worker = Worker::new(client, worker_url, master_url);

    let mut router = Router::new();
    router.register("/healthcheck".to_string(), healthcheck(worker.status.clone()));
    router.register("/task".to_string(), process_task(worker.status.clone(), worker.sender.clone()));


    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    debug!("Registering...");
    worker.register().await?;
    debug!("Request accepted. Listening...");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                router.process(stream).await;
            },
            Err(e) => {
                error!("{e}");
                break;
            }
        }
    }

    Ok(())
}
