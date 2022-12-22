# Simple cluster

An implementation of a distributed system which read files at given S3 location and counts occurrence of each word. The system consist of two elements Master and Worker nodes. The Master node is provides an API interface where we can check the current state of the system and request task processing. The Worker node is initially registering itself to the cluster and then waiting for tasks. The master node verifies the status of the Worker nodes, and removing them from the system if is unable to communicate with given worker. In case of broken worker, the Kubernetes infrastructure, will take care of spawning new worker, that will join the system automatically.

## Build and installation

For building the nodes, `Rust nightly` may be required.  
Both nodes can be build from the `root` directory with following commands:  
* `docker build -f ./master-node/Dockerfile . -t master-node:<version-tag>`
* `docker build -f ./worker-node/Dockerfile . -t worker-node:<version-tag>`

You can use the Helm chart to install the system to Kubernetes `helm install --set aws_access_key_id=<aws-access-key-id --set aws_secret_access_key=<aws-access-key-id> <name> simple-cluster-helm-chart/`.  
To delete it use `helm delete <name>`

## API
* `\status GET` - Return info of the each worker node address and status
* `\task POST` - Request word counting task from selected S3 bucket. Require `bucket name` being provided as a text in request body.
