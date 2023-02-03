import sys
import requests
import pandas as pd


def search(db_conn_url: str, range_min: str, range_max: str) -> dict:
    """Fetch filtered data from the database

    Args:
        db_conn_url (str): Database connection URI
        range_min (str): Minimum value to search
        range_max (str): Maximum value to search

    Returns:
        dict: JSON Response of filtered data
    """
    resp = requests.get(
        db_conn_url,
        params={
            "orderBy": '"Avg AQI"',
            "startAt": range_min,
            "endAt": range_max,
        },
    )
    return resp.json()


def restucture_data(data: dict) -> pd.DataFrame:
    """Restucture and format data

    Args:
        data (dict): Input JSON data

    Returns:
        pd.DataFrame: Formatted dataframe
    """
    df = pd.DataFrame(data.values())
    df = df.loc[:, ["Country", "Month", "Year"]]
    df.sort_values(by=["Country", "Month", "Year"], inplace=True, ignore_index=True)
    return df


def parse_args(line: list[str]) -> dict:
    """Parse the command line arguments

    Args:
        line (str): Input command line

    Returns:
        dict: Arguments dictionary
    """
    args = {
        "file": line[0],
        "db_conn_url": line[1],
        "range_min": line[2],
        "range_max": line[3],
    }
    return args


if __name__ == "__main__":
    """To run the file execute the command
    python search.py https://test-5681a-default-rtdb.firebaseio.com/aqi.json 20 30
    """
    args = parse_args(sys.argv)
    data = restucture_data(
        search(args["db_conn_url"], args["range_min"], args["range_max"])
    )
    print(data)
