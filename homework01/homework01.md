# Question 1
In terminal:
docker run -it --entrypoint=bash python:3.13
pip --version

The answer is 25.3.

# Question 2
All services in the same docker-compose.yaml are on the same Docker network by default. Each container can reach the other using its service name or container name as the hostname.

The answer would be postgres:5432 and db:5432.

# Question 3
