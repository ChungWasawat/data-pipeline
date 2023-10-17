# Prefect 

### Prefect Deployment
- `prefect init` to initialize a prefect project
- `prefect cloud login` to login to prefect cloud or `prefect server start` to start a local server
- `prefect work-pool create my-pool` to create a work pool on prefect server
- `prefect worker start -p my-pool -t process` to start a worker on the pool
- `prefect deploy python_file.py:flow_function -n 'deployment_name' -p my-pool` to create deployment on prefect server
- `prefect deployment run flow_function/deployment_name` to run the deployment