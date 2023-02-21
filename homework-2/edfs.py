import json
import sys
import uuid

import requests
import traceback


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

    def get(self, endpoint, params=None):
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
        self.parent = action_item
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

    def _dir_exists(self) -> bool:
        """Checks if the directory exists."""
        if self.get(f"{self.action_item}.json"):
            return True
        return False

    def _file_exists(self) -> bool:
        """Checks if the file exists."""
        file_endpoint = self.action_item.split(".")[0]
        if self.get(f"{file_endpoint}.json"):
            return True
        return False

    def _is_dir_empty(self) -> bool:
        """Checks if the folder is empty."""
        if len(self.get(f"{self.action_item}.json").keys()) > 3:
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

    def ls(self):
        try:
            if self.action_item == "/":
                res = self.get("/.json")
            else:
                res = self.get(f"{self.action_item}.json")
            print("\t".join(self.top_level_parser(res)))
        except Exception as e:
            print("Error", traceback.format_exc())

    def mkdir(self):
        try:
            assert self.action_item.startswith("/"), "Path must start with /"
            if not self._dir_exists():
                self.put(
                    self.action_item,
                    data={
                        "type": "DIR",
                        "name": self.action_item.split("/")[-1],
                        "id": uuid.uuid4().hex,
                    },
                )
                print(f"Successfully created directory: {self.action_item}")
            else:
                print(f"Directory already exists: {self.action_item}")
        except Exception as e:
            print(f"Error: {e}")

    def rmdir(self):
        try:
            if self._is_dir_empty():
                self.delete(f"{self.action_item}.json")
                print(f"Successfully deleted directory: {self.action_item}")
            else:
                print(f"Directory is not empty: {self.action_item}")
        except Exception as e:
            print(f"Directory does not exist: {self.action_item}")
            print(f"Error: {e}")

    def create(self):
        try:
            if not self._file_exists():
                self.put(
                    self.action_item.split(".")[0],
                    data={
                        "type": "FILE",
                        "name": self.action_item.split("/")[-1],
                        "id": uuid.uuid4().hex,
                        "content": "hello world",
                    },
                )
                print(f"Successfully created file: {self.action_item}")
            else:
                print(f"File already exists: {self.action_item}")
        except Exception as e:
            print(f"Error: {e}")

    def rm(self):
        file_endpoint = self.action_item.split(".")[0]
        if self._file_exists():
            self.delete(f"{file_endpoint}.json")
            print(f"Successfully deleted file: {self.action_item}")
        else:
            print(f"File does not exist: {self.action_item}")

    def export(self):
        ...

    def execute(self):
        try:
            if self.command in self.function_list.keys():
                self.function_list[self.command]()
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
