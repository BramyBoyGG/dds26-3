FROM python:3.12-slim

WORKDIR /home/flask-app

# Build context is the project root (set in docker-compose.yml)
# so we can copy both the service code and the shared packages.
COPY order/requirements.txt .

RUN pip install -r requirements.txt

# Copy the order service code
COPY order/ .

# Copy the shared common package so "from common import ..." works
COPY common/ ./common/

# Copy the orchestrator library so "from orchestrator import ..." works
COPY orchestrator/ ./orchestrator/

EXPOSE 5000
