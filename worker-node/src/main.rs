use std::sync::mpsc::Sender;
use std::error::Error;
use std::env;
use std::sync::{mpsc, Arc, Mutex};
use std::{thread, time};

use tokio::net::TcpListener;
use tokio::task;

use reqwest::Client;
use reqwest::header::CONTENT_LENGTH;

use http_request::{Route, Router};

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
    sender: mpsc::Sender<String>,
    status: Arc<Mutex<Status>>,
}

impl Worker {
    fn new(client: Client, url: String, master_url: String) -> Worker {
        let (sender, receiver) = mpsc::channel();
        let master_url_clone = master_url.clone();
        let status = Arc::new(Mutex::new(Status::IDLE));
        let t_status = status.clone();

        thread::spawn(move || {
            loop {
                let client = reqwest::blocking::Client::new();
                let task = receiver.recv().unwrap();
                info!("Task {task}");
                thread::sleep(time::Duration::from_secs(5));
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
            info!("Processing...");
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
    let ip_addr = env::var("IP_ADDRES").unwrap_or("0.0.0.0".to_string());
    let port = env::var("PORT_NUMBER").unwrap_or("5101".to_string());
    let addr = format!("{}:{}", ip_addr, port);

    let client = Client::new();
    let worker_url = format!("http://localhost:{port}");
    let master_url = env::var("MASTER_URL").unwrap();
    let worker = Worker::new(client, worker_url, master_url);

    let mut router = Router::new();
    router.register("/healthcheck".to_string(), healthcheck(worker.status.clone()));
    router.register("/task".to_string(), process_task(worker.status.clone(), worker.sender.clone()));

    info!("Registering...");
    worker.register().await?;
    info!("Request accepted. Listening...");

    let listener = TcpListener::bind(&addr).await?;

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                router.process(stream).await;
            },
            Err(e) => {
                eprintln!("{e}");
                break;
            }
        }
    }

    Ok(())
}
