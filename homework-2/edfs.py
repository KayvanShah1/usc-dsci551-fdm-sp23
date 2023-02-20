import base64
import fileinput
import json
import sys
import uuid

import requests


class FirebaseConfig:
    base_uri = "https://test-5681a-default-rtdb.firebaseio.com"


class FirebaseClient:
    def __init__(self):
        self.base_uri = FirebaseConfig.base_uri

    def put(self, data):
        ...

    def post(self, data):
        ...

    def get(self):
        ...

    def delete(self):
        ...


class HDFSEmulator(FirebaseClient):
    def __init__(self, command: str, action_item: str):
        """_summary_

        Args:
            command (str): _description_
            action_item (str): _description_
        """
        self.command = command
        self.action_item = action_item

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
        assert self.command.startswith("-"), "Command must start with -"

    def ls(self):
        ...

    def mkdir(self):
        try:
            assert self.action_item.startswith("/"), "Path must start with /"
            requests.put()
        except Exception as e:
            print(f"Error: {e}")

    def rmdir(self):
        ...

    def create(self):
        ...

    def rm(self):
        ...

    def export(self):
        ...

    def execute(self):
        try:
            self.function_list[self.command]()
        except Exception as e:
            print(f"Command not found: {self.command}")
            print(f"Error: {e}")


def dict2xml(obj: dict, line_padding: str = "") -> str:
    """Converts a dictionary to a XML string

    Args:
        obj (dict): Dictionary to convert
        line_padding (str, optional): Line Padding to be used. Defaults to "".

    Returns:
        str: XML string representation of dictionary
    """
    res = list()
    obj_type = type(obj)

    if obj_type is dict:
        for tag_name in obj:
            sub_obj = obj[tag_name]
            res.append(f"{line_padding}<{tag_name}>")
            res.append(dict2xml(sub_obj, "\t" + line_padding))
            res.append(f"{line_padding}<{tag_name}>")
        return "\n".join(res)

    if obj_type is list:
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
