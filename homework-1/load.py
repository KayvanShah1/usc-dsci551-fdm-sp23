import sys
import json
import requests


def push_to_firebase(db_url: str, data: dict) -> None:
    """Push the data to the Firebase Realtime database

    Args:
        db_url (str): DB connection string or URL to connect to
        data (dict): Payload
    """
    try:
        requests.put(db_url, json.dumps(data))
        print("Successfully pushed data to firebase")
    except Exception as e:
        print(f"Exception occured while pushing data to firebase: {e}")


def read_data(path: str) -> dict:
    """Reads the data from file path

    Args:
        path (str): File path

    Returns:
        dict: Output dict
    """
    try:
        with open(path, "rb") as f:
            doc = json.load(f)
        return doc
    except FileNotFoundError as e:
        print(e)


def parse_args(line: str) -> dict:
    """Parse the command line arguments

    Args:
        line (str): Input command line

    Returns:
        dict: Arguments dictionary
    """
    args = {"file": line[0], "source": line[1], "destination": line[2]}
    return args


if __name__ == "__main__":
    """To run the file execute the command
    python load.py aqi.json https://test-5681a-default-rtdb.firebaseio.com/aqi.json"""
    args = parse_args(sys.argv)
    data = read_data(args["source"])
    push_to_firebase(args["destination"], data)
