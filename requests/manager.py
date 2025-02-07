from __future__ import annotations
import argparse
import requests
from requests import models

parser = argparse.ArgumentParser()
parser.add_argument("-a", "--action", type=str, help="Choose action", required=True)
args = parser.parse_args()

def start_task() -> models.Response:
    url = "http://localhost:8080/tasks"
    headers = {
        "Content-Type": "application/json"
    }
    data = None
    with open("add_task.json", "rb") as file:
        data = file.read()
    return requests.post(
        url,
        data=data,
        headers=headers)

def stop_task():
    id = "bb1d59ef-9fc1-4e4b-a44d-db571eeed203"
    url = f"http://localhost:8080/tasks/{id}"
    response = requests.delete(url)
    return response


actions = {
    "start-task": start_task,
    "stop-task": stop_task
}

def main():
    action = actions[args.action]
    response = action()
    print(response)
    response.raise_for_status()

main()