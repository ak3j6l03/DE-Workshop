## Question 1
In terminal: <br>
docker run -it --entrypoint=bash python:3.13 <br>
pip --version

The answer is 25.3.

## Question 2
All services in the same docker-compose.yaml are on the same Docker network by default. Each container can reach the other using its service name or container name as the hostname.

The answer would be postgres:5432 and db:5432.

## Question 3 to 6
In terminal: <br>
uv init --python=3.13 <br>
uv add pandas pyarrow <br>
uv add --dev jupyter <br>
uv run jupyter notebook <br>

The answers are in check_data.ipynb.

## Question 7
terraform init, terraform apply -auto-approve, terraform destroy