import argparse
import requests
import logging
import json

def start_task():
    url = "http://localhost:8081/tasks"
    headers = {
        "Content-Type": "application/json"
    }
    data = None
    with open("add_task.json", "rb") as file:
        data = file.read()
    response = requests.post(
        url,
        data=data,
        headers=headers)
    response.raise_for_status()
    print(response)
    task_event = json.loads(data)
    print(f"Task Id: {task_event["Task"]["ID"]}")

def stop_task():
    id = "bb1d59ef-9fc1-4e4b-a44d-db571eeed203"
    url = f"http://localhost:8081/tasks/{id}"
    response = requests.put(url)
    response.raise_for_status()

def delete_task():
    id = "bb1d59ef-9fc1-4e4b-a44d-db571eeed203"
    url = f"http://localhost:8081/tasks/{id}"
    response = requests.delete(url)
    response.raise_for_status()

def main():
    logging.basicConfig(level=logging.INFO, 
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
                        handlers=[
                            logging.FileHandler("logs.log"), 
                            logging.StreamHandler()])
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--action", type=str, help="Choose action", required=True)
    args = parser.parse_args()
    
    actions = {
        "start-task": start_task,
        "stop-task": stop_task,
        "delete-task": delete_task
    }
    action = actions.get(args.action, None)
    if not action:
        logging.info(f"No action. Expected: {actions.keys()}")
    else:
        action()

main()