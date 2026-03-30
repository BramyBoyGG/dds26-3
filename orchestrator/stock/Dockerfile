FROM python:3.12-slim

WORKDIR /home/flask-app

# Build context is the project root (set in docker-compose.yml)
COPY ./stock/requirements.txt .

RUN pip install -r requirements.txt

# Copy the stock service code
COPY ./stock/ .

# Copy the shared common package
COPY ./common ./common/

# Copy the orchestrator library
COPY ./orchestrator/ ./orchestrator/

EXPOSE 5000
