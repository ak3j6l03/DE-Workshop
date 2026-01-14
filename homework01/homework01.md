## Question 1
In terminal:
docker run -it --entrypoint=bash python:3.13
pip --version

The answer is 25.3.

## Question 2
All services in the same docker-compose.yaml are on the same Docker network by default. Each container can reach the other using its service name or container name as the hostname.

The answer would be postgres:5432 and db:5432.

## Question 3 to 6
In terminal:
uv init --python=3.13
uv add pandas pyarrow
uv add --dev jupyter
uv run jupyter notebook

The answers are in check_data.ipynb.

## Question 7
terraform init, terraform apply -auto-approve, terraform destroy