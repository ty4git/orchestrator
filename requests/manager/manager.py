#! /usr/bin/python3

import argparse
import signal
import time
import requests
import subprocess
import logging

parser = argparse.ArgumentParser()
parser.add_argument("-a", "--action", type=str, help="Choose action", required=True)
args = parser.parse_args()

def start_task():
    url = "http://localhost:8080/tasks"
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

def stop_task():
    id = "bb1d59ef-9fc1-4e4b-a44d-db571eeed203"
    url = f"http://localhost:8080/tasks/{id}"
    response = requests.delete(url)
    response.raise_for_status()

def run_all():
    worker = Process("worker.log", "worker", "localhost", "8081")
    manager = Process("manager.log", "manager", "localhost", "8080")

    try:
        worker.run()
        manager.run()
        signal.signal(signal.SIGINT,
                    lambda signum, frame: terminate(signum, worker, manager))
        while True:
            time.sleep(1)
    except Exception as e:
        logging.error(f"Error: \"{e}\"")
    finally:
        terminate("No signal", worker, manager)

actions = {
    "start-task": start_task,
    "stop-task": stop_task,
    "run-all": run_all
}

class Process:
    def __init__(self, output_file: str, name: str, host: str, port: str):
        self.output_file_name: str = output_file
        self.name = name
        self.host = host
        self.port = port
        self._output_file = None
        self._process = None
        
    def run(self):
        self._output_file = open(self.output_file_name, 'a')
        
        try:
            process = subprocess.Popen(["go", "run", ".", "-name", self.name],
                                        stdout=self._output_file, stderr=self._output_file,
                                        cwd="../..")
            self._process = process

            while process.poll() is None:
                if self._is_ready(self.host, self.port):
                    break
                time.sleep(1)

            if process.returncode is None:
                logging.info(f"\"{self.name}\" is running...")
            else:
                logging.error(f"Error code: \"{process.returncode}\". Check error logs")
                raise ValueError()
        except Exception as e:
            logging.error(f"Error: \"{e}\"")
            self.terminate()
            raise

    def wait(self):
        if self._process:
            self._process.wait()
    
    def terminate(self):
        if self._process is not None:
            self._process.terminate()
            return_code = self._process.wait()
            self._process = None

            if return_code == 0:
                logging.info("Process finished")
            else:
                logging.error(f"Proccess finished with error code: \"{return_code}\"")

        if self._output_file is not None:
            self._output_file.close()
    
    def _is_ready(self, host: str, port: str) -> bool:
        logging.info(f"Checking app \"{host}:{port}\"...")
        status = False
        try:
            response = requests.get(f"http://{host}:{port}", timeout=5)
            response.raise_for_status()
            status = True
        except requests.exceptions.HTTPError as http_err:
            print(f"Http error: {http_err}")
            status = True
        except requests.exceptions.ConnectionError as conn_err:  # Ошибки соединения
            print(f"Error connection: {conn_err}")
            status = False
        except requests.exceptions.Timeout as timeout_err:  # Тайм-аут
            print(f"Timeout: {timeout_err}")
            status = False
        except Exception as e:
            print(f"Unknown error: {e}")
            status = False
        
        msg = "App is ready" if status else "App is not ready"
        logging.info(msg)
        return status

def terminate(sig, worker, manager):
    logging.info(f"Signal: \"{sig}\". Terminating application...")
    worker.terminate()
    manager.terminate()
    exit(0)

def main():
    logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s", 
                    handlers=[logging.FileHandler("logs.log"), 
                                logging.StreamHandler()])
    
    action = actions.get(args.action, None)
    if not action:
        logging.info(f"No action. Expected: {actions.keys()}")
    else:
        action()

main()