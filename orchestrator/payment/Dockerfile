FROM python:3.12-slim

WORKDIR /home/flask-app

# Build context is the project root (set in docker-compose.yml)
COPY ./payment/requirements.txt .

RUN pip install -r requirements.txt

# Copy the payment service code
COPY ./payment/ .

# Copy the shared common package
COPY ./common/ ./common/

# Copy the orchestrator library
COPY ./orchestrator/ ./orchestrator/

EXPOSE 5000
