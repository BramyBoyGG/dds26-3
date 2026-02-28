# Shopping Cart System 

### High Level Architecture 


Our project implements a reactive microservice architecture with an orchestrated SAGA pattern for distributed transactions. We use Redis streams as our message broker.
#### Notation used in diagrams
- **XADD** — Append a message to a stream. Used by services to publish commands and responses (e.g., `XADD stock-commands {tx_id, cmd=RESERVE_STOCK, items}`).
- **XREADGROUP** — Read messages from a stream as part of a consumer group.
- **XACK** — Acknowledge a message has been processed. Unacknowledged messages remain in the Pending Entries List (PEL) and are redelivered on consumer restart.
- **tx_id** — Transaction ID. A unique identifier generated per checkout, used as the idempotency key across all saga steps.
- **tx:{tx_id}** — The transaction log entry stored in `order-db`, tracking the current state of the saga (e.g., `STARTED`, `RESERVING_STOCK`, `COMPLETED`, `ABORTED`).

![Architecture](diagrams/architecture.png) 

### Transaction State Machine 

![Transaction State Machine](diagrams/state_machine.png) 

For detailed step-by-step checkout flows, see the [Appendix: Sequence Diagrams](#appendix-sequence-diagrams).

### Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `helm-config` 
   Helm chart values for Redis and ingress-nginx
        
* `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
    
* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

***Requirements:*** You need to have docker and docker-compose installed on your machine. 

K8s is also possible, but we do not require it as part of your submission. 

#### minikube (local k8s cluster)

This setup is for local k8s testing to see if your k8s config works before deploying to the cloud. 
First deploy your database using helm by running the `deploy-charts-minicube.sh` file (in this example the DB is Redis 
but you can find any database you want in https://artifacthub.io/ and adapt the script). Then adapt the k8s configuration files in the
`\k8s` folder to mach your system and then run `kubectl apply -f .` in the k8s folder. 

***Requirements:*** You need to have minikube (with ingress enabled) and helm installed on your machine.

#### kubernetes cluster (managed k8s cluster in the cloud)

Similarly to the `minikube` deployment but run the `deploy-charts-cluster.sh` in the helm step to also install an ingress to the cluster. 

***Requirements:*** You need to have access to kubectl of a k8s cluster.

---

## Appendix: Sequence Diagrams

### Checkout — Happy Path

All SAGA steps succeed: stock is reserved, payment is deducted, and the order is marked as paid.

![Happy Path](diagrams/happy_path.png)

### Checkout — Insufficient Stock

Stock reservation fails. Since no prior steps succeeded, no compensation is needed. The transaction is aborted.

![Insufficient Stock](diagrams/stock_insufficient.png)

### Checkout — Payment Failure

Stock reservation succeeds but payment fails. The orchestrator triggers a compensating transaction to restore the reserved stock, then aborts.

![Payment Failure](diagrams/payment_failure.png)
