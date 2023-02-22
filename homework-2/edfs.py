import json
import sys
import uuid

import requests


class FirebaseConfig:
    base_uri = "https://test-5681a-default-rtdb.firebaseio.com"


class FirebaseClient:
    def __init__(self):
        self.base_uri = FirebaseConfig.base_uri

    def put(self, path: str, data: dict):
        res = requests.put(
            f"{self.base_uri}{path}.json",
            data=json.dumps(data),
        )
        return res.json()

    def get(self, endpoint):
        res = requests.get(f"{self.base_uri}{endpoint}")
        return res.json()

    def delete(self, endpoint):
        res = requests.delete(f"{self.base_uri}{endpoint}")
        return res.json()


class HDFSEmulator(FirebaseClient):
    def __init__(self, command: str, action_item: str):
        """_summary_

        Args:
            command (str): _description_
            action_item (str): _description_
        """
        self.command = command
        self.action_item = action_item
        self.parent = self._get_parent_dir(action_item)
        self._metadata_vars = ["type", "id"]

        self._verify_input_command()

        self.function_list = {
            "-ls": self.ls,
            "-mkdir": self.mkdir,
            "-rmdir": self.rmdir,
            "-create": self.create,
            "-rm": self.rm,
            "-export": self.export,
        }

        super().__init__()

    def _verify_input_command(self):
        """Verify the format of input command"""
        assert self.command.startswith("-"), "Command must start with -"

    def _dir_exists(self, path) -> bool:
        """Checks if the directory exists."""
        if self.get(f"{path}.json"):
            return True
        return False

    def _file_exists(self, path) -> bool:
        """Checks if the file exists."""
        file_endpoint = path.split(".")[0]
        if self.get(f"{file_endpoint}.json"):
            return True
        return False

    def _is_dir_empty(self, path) -> bool:
        """Checks if the folder is empty."""
        if len(self.get(f"{path}.json").keys()) > 3:
            return False
        return True

    def _get_parent_dir(self, path: str) -> str:
        """Get the parent directory"""
        return "/".join(path.split("/")[:-1])

    def top_level_parser(self, json_doc: dict) -> list:
        """_summary_

        Args:
            json_doc (dict): _description_

        Returns:
            list: _description_
        """
        parsed_list = []
        for key, value in json_doc.items():
            if type(value) is dict:
                if "type" in value.keys():
                    parsed_list.append(
                        value["name"]
                        if value["type"] == "FILE"
                        else f"/{value['name']}"
                    )
                else:
                    parsed_list.append(f"/{key}")
        return parsed_list

    def ls(self, path):
        try:
            if path == "/":
                res = self.get("/.json")
            else:
                res = self.get(f"{path}.json")
            print("\t".join(self.top_level_parser(res)))
        except Exception as e:
            print("Error", e)

    def mkdir(self, path):
        try:
            assert path.startswith("/"), "Path must start with /"

            # Check and create user directory
            if len("/".join(path.split("/"))) == 3:
                print("User dir not found. Creating one...")
                user_dir = "/".join(path.split("/")[:2])
                self.mkdir(user_dir)

            # Create dir
            if not self._dir_exists(path):
                self.put(
                    path,
                    data={
                        "type": "DIR",
                        "name": path.split("/")[-1],
                        "id": uuid.uuid4().hex,
                    },
                )
                print(f"Successfully created directory: {path}")
            else:
                print(f"Directory already exists: {path}")
        except Exception as e:
            print(f"Error: {e}")

    def rmdir(self, path):
        try:
            if self._is_dir_empty(path):
                self.delete(f"{path}.json")
                print(f"Successfully deleted directory: {path}")
            else:
                print(f"Directory is not empty: {path}")
        except Exception as e:
            print(f"Invalid path: {path}")

    def create(self, path: str):
        try:
            if self._dir_exists(self._get_parent_dir(path)):
                if not self._file_exists(path):
                    self.put(
                        path.split(".")[0],
                        data={
                            "type": "FILE",
                            "name": path.split("/")[-1],
                            "id": uuid.uuid4().hex,
                            "content": "hello world",
                        },
                    )
                    print(f"Successfully created file: {path}")
                else:
                    print(f"File already exists: {path}")
            else:
                print(f"Invalid Path: {path}")
        except Exception as e:
            print(f"Error: {e}")

    def rm(self, path: str):
        file_endpoint = path.split(".")[0]
        if self._file_exists(path):
            self.delete(f"{file_endpoint}.json")
            print(f"Successfully deleted file: {path}")
        else:
            print(f"File does not exist: {path}")

    def export(self):
        ...

    def execute(self):
        try:
            if self.command in self.function_list.keys():
                self.function_list[self.command](self.action_item)
            else:
                print(f"Command not found: {self.command}")
                print(f"Available Commands: {list(self.function_list.keys())}")
        except Exception as e:
            print(
                f"Error occured while exceuting the command, check the arguments.\n{e}"
            )


def dict2xml(obj: dict, line_padding: str = "") -> str:
    """Converts a dictionary to a XML string

    Args:
        obj (dict): Dictionary to convert
        line_padding (str, optional): Line Padding to be used. Defaults to "".

    Returns:
        str: XML string representation of dictionary
    """
    res = list()
    elem_type = type(obj)

    if elem_type is dict:
        for tag_name in obj:
            sub_elem = obj[tag_name]
            res.append(f"{line_padding}<{tag_name}>")
            res.append(dict2xml(sub_elem, "\t" + line_padding))
            res.append(f"{line_padding}<{tag_name}>")
        return "\n".join(res)

    if elem_type is list:
        for sub_elem in obj:
            res.append(dict2xml(sub_elem, line_padding))
        return "\n".join(res)

    return f"{line_padding}{obj}"


def parse_args(line: list[str]) -> dict:
    """Parse the command line arguments

    Args:
        line (str): Input command line

    Returns:
        dict: Arguments dictionary
    """
    args = {"file": line[0], "command": line[1], "action_item": line[2]}
    return args


if __name__ == "__main__":
    """To run the file execute the command
    python edfs.py <command> <action-item>
    """
    args = parse_args(sys.argv)
    fs = HDFSEmulator(args["command"], args["action_item"])
    fs.execute()
