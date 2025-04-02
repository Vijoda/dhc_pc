import requests
import json
from typing import Literal


def start_auto_retry(
    uc_upload_id: str,
    max_retry_attempts: int,
    retry_minutes_interval: int,
    requeue_worker_type: str,
) -> dict | None:
    url = "https://c7fhe7bjidnvyiqubuichypvoy0swjqb.lambda-url.ap-south-1.on.aws/"
    payload = {
        "uc_upload_id": uc_upload_id,
        "retry_minutes_interval": retry_minutes_interval,
        "max_retry_attempts": max_retry_attempts,
        "requeue_worker_type": requeue_worker_type,
    }
    json_data = json.dumps(payload)

    try:
        # response = requests.get(url, params=params)
        response = requests.post(url, json=json_data)
        response.raise_for_status()  # This will raise an exception for HTTP errors
        data = response.json()
        print(f"{data=}")
        return data
    except requests.exceptions.HTTPError as error:
        # error.response.status_code == 429
        print(f"HTTP error: {error}")

    except Exception as err:
        print(f"Error: {err}")


def check_status(uc_upload_id: str) -> Literal["NOT IN DB", "In Progress", "Completed"]:
    url = "https://c7fhe7bjidnvyiqubuichypvoy0swjqb.lambda-url.ap-south-1.on.aws/"
    params = {"uc_upload_id": uc_upload_id}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        # print(response.json())# This will raise an exception for HTTP errors
        # print(response)
        return response.text
    except requests.exceptions.HTTPError as error:
        # error.response.status_code == 429
        print(f"HTTP error: {error}")

    except Exception as err:
        print(f"Error: {err}")



def start_uc_after_google(
    uc_upload_id: str,
    requeue_worker_type: str,
) -> dict | None:
    url = "https://c7fhe7bjidnvyiqubuichypvoy0swjqb.lambda-url.ap-south-1.on.aws/"
    payload = {
        "uc_upload_id": uc_upload_id,
        "requeue_worker_type": requeue_worker_type,
    }
    json_data = json.dumps(payload)

    try:
        # response = requests.get(url, params=params)
        response = requests.post(url, json=json_data)
        string_result = response.content.decode('utf-8')
        print(string_result)
        if string_result == 'NOT IN DB':
            return {'status': 'NOT IN DB'}
        response.raise_for_status()  # This will raise an exception for HTTP errors
        data = response.json()
        print(f"{data=}")
        return data
    except requests.exceptions.HTTPError as error:
        # error.response.status_code == 429
        print(f"HTTP error: {error}")

    except Exception as err:
        print(f"Error: {err}")


# upload_id='mvZkGcHtpzyoQy2LxUn6XP_UC'
# requeue_worker_type = 'NOVPN_UC'

# start_uc_after_google(upload_id,requeue_worker_type)