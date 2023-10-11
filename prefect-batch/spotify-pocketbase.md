# A markdown file to explain the spotify-pocketbase.py 
This pipeline is a simple pipeline to scrape artist data through spotify api and store the data in a database in pocketbase.

### Tech
- Python v3.8.18
- Prefect v2.13.2: a module to do data orchestration
- spotipy v2.23.0: Python module to call Spotify API
- pocketbase v0.9.2: Python module to call Pocketbase API

### Prefect Deployment
- `prefect init` to initialize a prefect project
- `prefect cloud login` to login to prefect cloud or `prefect server start` to start a local server
- `prefect work-pool create my-pool` to create a work pool on prefect server
- `prefect worker start -p my-pool -t process` to start a worker on the pool
- `prefect deploy python_file.py:flow_function -n 'deployment_name' -p my-pool` to create deployment on prefect server
- `prefect deployment run python_file.py:flow_function` to run the deployment