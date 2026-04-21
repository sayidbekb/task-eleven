from airflow.decorators import task
import requests
import os

@task
def download_file(url: str, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    r = requests.get(url)
    r.raise_for_status()

    with open(path, "wb") as f:
        f.write(r.content)

    return path