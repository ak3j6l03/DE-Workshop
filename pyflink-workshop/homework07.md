## Steps in q4
``` bash
uv init -p 3.12
uv add kafka-python pandas pyarrow

docker compose build
docker compose up -d

docker exec -it pyflink-workshop-redpanda-1 rpk topic create green-trips

uv run python src/producers/producer.py

uvx pgcli -h localhost -p 5432 -U postgres -d postgres

CREATE TABLE green_trips_aggregated (
    window_start TIMESTAMP,
    pulocationid INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, pulocationid)
);

docker compose exec jobmanager ./bin/flink run \
-py /opt/src/job/pickup_location.py \
--pyFiles /opt/src -d
```

## Question 1
![alt text](q1.png)

## Question 2
![alt text](q2.png)

## Question 3
![alt text](q3.png)

## Question 4
![alt text](q4.png)

## Question 5
![alt text](q5.png)

## Question 6
![alt text](q6.png)