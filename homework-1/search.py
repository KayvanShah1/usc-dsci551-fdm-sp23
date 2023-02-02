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


if __name__ == "__main__":
    """To run the file execute the command
    python search.py
    """
    args = parse_args(sys.argv)
