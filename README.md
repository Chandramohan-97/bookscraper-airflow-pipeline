## Description

This project is an Apache Airflow–based data pipeline that scrapes book information from **books.toscrape.com**, applies basic transformations, and stores the processed output locally.

The project focuses on:
- Clean Airflow project structure
- Separating DAG definitions from core pipeline logic
- Running Airflow with Docker and Docker Compose
- Using PostgreSQL as the Airflow metadata database

---

## How to Run Locally

### Prerequisites
- Docker
- Docker Compose

> If Docker requires admin permissions on your system, use `sudo` for the commands below.

---

### Steps

#### 1. Clone the repository
```bash
git clone https://github.com/Chandramohan-97/bookscraper-airflow-pipeline.git
```
#### 2. Navigate into the project directory
```bash
cd bookscraper-airflow-pipeline
```
#### 3. Give required permissions (Linux only)
``` bash
chmod -R 777 .
```
#### 4. Move to the Docker directory
``` bash
cd Docker
```
#### 5. Start Airflow using Docker Compose
``` bash
docker compose up
```
Wait until the webserver and scheduler containers are running.

### Access Airflow UI
Open your browser and visit:

``` arduino
http://localhost:8082
```
Login credentials:
```
Username: admin

Password: admin@123
```
### Stop and Clean Up
To stop containers:

```bash
docker compose down
```
To stop containers and remove volumes:

``` bash
docker compose down -v
```