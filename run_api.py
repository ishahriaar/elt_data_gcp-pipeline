import requests
import json
ENDPOINT = "https://dummyjson.com/"


def make_api_call(resource):
    ENDPOINT = "https://dummyjson.com/"
    results_picked = 0
    total_results = 100
    all_data = []
    while results_picked < total_results:
        response = requests.get(f"{ENDPOINT}{resource}", params={"skip": results_picked})
        if response.status_code == 200:
            data = response.json()
            rows = data.get(resource)
            print(len(rows))
            all_data += rows  # concatening the two lists
            total_results = data.get("total")
            results_picked += len(rows)  # to skip them in the next call
        else:
            raise Exception(response.text)
    return all_data


users_data = make_api_call("users")
carts_data = make_api_call("carts")


def download_json(data, resource_name):
    file_path = f"{resource_name}.json"
    with open(file_path, "w") as file:
        file.write("\n".join([json.dumps(row) for row in data]))


download_json(carts_data, "carts")
download_json(users_data, "users")
