<img src="https://leaders2021.innoagency.ru/static/img/general/logo.svg"
  style="height: 80px;">

# Sport Object Analysis

## Database
1 Depoly Postgres with PostGIS extenstion using docker
```bash
$ docker pull kartoza/postgis
$ docker run --name=postgis -d -e POSTGRES_USER=<username> -e POSTGRES_PASSWORD=<password> -e POSTGRES_DBNAME=<dbname> -e ALLOW_IP_RANGE=0.0.0.0/0 -p 5432:5432 --restart=always postgis/postgis
```

## Fill Database
1. Clone repo and "cd" into it
```bash
$ git clone https://github.com/techpotion/leaders2021-utils.git
$ cd leaders2021-utils
```

2. Fill .env file with database connection data according to .env.example file
```bash
DB_HOST=localhost
DB_PORT=5432
DB_USERNAME=postgis
DB_PASSWORD=postgis
DB_DB=postgis
```

3. Make virtual environment (optional) and install project's dependencies
```bash
$ python3 -m venv env
$ source env/bin/activate
$ pip install -r requirements.txt
```

4 Run the script
```bash
$ python main.py
```
