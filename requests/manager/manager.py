#! ./.venv/bin/python

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
    id = "21b23589-5d2d-4731-b5c9-a97e9832d021"
    url = f"http://localhost:8080/tasks/{id}"
    response = requests.delete(url)
    response.raise_for_status()

def run_all():
    worker = Process("worker.log", "worker", "localhost", "8081")
    manager = Process("manager.log", "manager", "localhost", "8080")

    def terminate(signum: signal.Signals, worker: Process, manager: Process):
        logging.info(f"Signal: \"{str(signum)}\". Terminating application...")
        worker.terminate()
        manager.terminate()

    try:
        worker.run()
        manager.run()
        signal.signal(signal.SIGINT,
                    lambda _, __: terminate(signal.SIGINT, worker, manager))
        
        while manager.is_running():
            time.sleep(1)

        time.sleep(1)

    except KeyboardInterrupt as e:
        logging.error(f"Error: \"{e}\"")
        terminate(signal.SIGINT, worker, manager)

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
            process = subprocess.Popen(["dlv", "debug" "--headless", "--listen=:2345", "--log", "--", f"-name={self.name}"],
                                        stdout=self._output_file,
                                        stderr=self._output_file,
                                        cwd="../..")
            self._process = process

            while not self._is_ready(self.host, self.port):
                time.sleep(5)

            if process.returncode is None:
                logging.info(f"\"{self.name}\" is running...")
            else:
                logging.error(f"Error code: \"{process.returncode}\". Check error logs")
                raise ValueError()
        except BaseException as e:
            logging.error(f"Error: \"{e}\"")
            self.terminate()
            raise

    def wait(self):
        if not self._process:
            raise ValueError()
        self._process.wait()
    
    def terminate(self):
        if self._process:
            logging.info(f"Finishing process of \"{self.name}\"")
            self._process.terminate()
            return_code = self._process.wait()
            self._process = None

            if return_code == 0:
                logging.info("Process finished")
            else:
                logging.error(f"Proccess finished with error code: \"{return_code}\"")

        if self._output_file is not None:
            self._output_file.close()

    def is_running(self):
        return self._process and self._process.returncode is None
    
        
    def _is_ready(self, host: str, port: str) -> bool:
        url = f"http://{host}:{port}"
        logging.info(f"Checking \"{self.name}\" \"{url}\"...")
        status = False
        try:
            response = requests.get(url, timeout=5)
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
        
        msg = f"\"{self.name}\" is ready" if status else f"\"{self.name}\" is not ready"
        logging.info(msg)
        return status

def main():
    logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
                    handlers=[logging.FileHandler("logs.log"), 
                                logging.StreamHandler()])
    
    action = actions.get(args.action, None)
    if not action:
        logging.info(f"No action. Expected: {actions.keys()}")
    else:
        action()

main()