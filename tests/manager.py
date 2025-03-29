import pytest
import requests
import json

BASE_URL = "http://localhost:8080"

def test_get_tasks():
    """Get tasks"""
    response = requests.get(url=f"{BASE_URL}/tasks")
    assert response.status_code == 200
    data = response.json()
    print(data)
    assert "id" in data
    assert data["id"] == 1

def test_start_task():
    """Start test task"""
    body = None
    with open("start_task.json", "r") as f:
        body = f.read()
    print(body)
    headers = {"Content-Type": "application/json"}
    response = requests.post(f"{BASE_URL}/tasks", data=body, headers=headers)
    response.raise_for_status()
    assert response.status_code == 201
    data = response.json()
    print(data)

# def test_put_resource():
#   '''Тест для проверки метода PUT'''
#   payload = {'name': 'Updated Resource'}
#   headers = {'Content-Type': 'application/json'}
#   response = requests.put(f"{BASE_URL}/resource/1", data=json.dumps(payload), headers=headers)
#   assert response.status_code == 200
#   data = response.json()
#   assert data['name'] == 'Updated Resource'

# def test_delete_resource():
#   '''Тест для проверки метода DELETE'''

#   response = requests.delete(f"{BASE_URL}/resource/1")
#   assert response.status_code == 204