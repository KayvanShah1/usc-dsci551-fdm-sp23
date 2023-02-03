import pandas as pd
import sys


def parse_args(line: list[str]) -> dict:
    """Parse the command line arguments

    Args:
        line (str): Input command line

    Returns:
        dict: Arguments dictionary
    """
    args = {"file": line[0], "source": line[1], "destination": line[2]}
    return args


def get_avg_aqi(filename: str) -> pd.DataFrame:
    """Calculates avg AQI value from given file

    Args:
        filename (str): Path to file where AQI data is stored

    Returns:
        pd.DataFrame: Aggregated dataframe
    """
    df = pd.read_csv(filename)
    df.drop_duplicates(inplace=True)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Year"] = df["Date"].dt.year
    df["Month"] = df["Date"].dt.month
    df = (
        df.groupby(by=["Country", "Year", "Month"])
        .mean(numeric_only=True)
        .reset_index()
        .rename(columns={"AQI Value": "Avg AQI"})
    )
    df.sort_values(by=["Country", "Year", "Month"], inplace=True)
    df["Avg AQI"] = df["Avg AQI"].round(1)
    return df


def save_as_json(df: pd.DataFrame, save_to: str):
    try:
        df.to_json(save_to, orient="records")
        print("Successfully saved %s" % save_to)
    except Exception as e:
        print("Error saving data to %s" % e)


if __name__ == "__main__":
    """To run the file execute the command
    python stat.py data/aqi.csv data/aqi.json
    OR
    python stat.py data/aqi.csv.zip data/aqi.json
    """
    args = parse_args(sys.argv)
    df = get_avg_aqi(args["source"])
    save_as_json(df, args["destination"])
