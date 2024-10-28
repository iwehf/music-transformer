import requests


def main():
    url = "http://127.0.0.1:8000/task"
    resp = requests.post(
        url,
        json={
            "worker_count": 3,
            "max_round": 500,
        },
    )
    resp.raise_for_status()
    resp_data = resp.json()
    task_id = resp_data["task_id"]
    print(task_id)


if __name__ == "__main__":
    main()
