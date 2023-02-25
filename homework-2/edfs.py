import json
import sys
import uuid

import requests


class FirebaseConfig:
    """Firebase Realtime DB config"""

    base_uri = "https://test-5681a-default-rtdb.firebaseio.com"


class FirebaseClient:
    """Firebase Client with GET, PUT & DELETE request functionalites"""

    def __init__(self):
        self.base_uri = FirebaseConfig.base_uri

    def put(self, path: str, data: dict) -> dict:
        """Sends a PUT request to Firebase DB API server to create file or directory

        Args:
            path (str): File or directory path
            data (dict): Metadata for file or directory

        Returns:
            dict: Request Feedback as JSON Response
        """
        res = requests.put(
            f"{self.base_uri}{path}.json",
            data=json.dumps(data),
        )
        return res.json()

    def get(self, endpoint: str):
        """Sends a GET request to Firebase Realtime DB to get list of files and directories
        and to find if a file or directory exists or not

        Args:
            endpoint (str): File or folder path

        Returns:
            dict: Request Feedback as JSON Response
        """
        res = requests.get(f"{self.base_uri}{endpoint}")
        return res.json()

    def delete(self, endpoint: str):
        """Sends a DELETE request to Firebase Realtime DB server to delete a file or directory

        Args:
            endpoint (str): File or folder path

        Returns:
            dict: Request Feedback as JSON Response
        """
        res = requests.delete(f"{self.base_uri}{endpoint}")
        return res.json()


class HDFSEmulator(FirebaseClient):
    """Emulate the file system structure of HDFS using Firebase and allow the export of its
    structure in the XML format.

    Args:
        FirebaseClient (class): Firebase API Client
    """

    def __init__(self, command: str, action_item: str):
        """Initialise the HDFS file system emulator with command and arguments

        Args:
            command (str): Input command
            action_item (str): Path
        """
        self.command = command
        self.action_item = action_item
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

    def _dir_exists(self, path: str) -> bool:
        """Checks if the directory exists."""
        if self.get(f"{path}.json"):
            return True
        return False

    def _file_exists(self, path: str) -> bool:
        """Checks if the file exists."""
        file_endpoint = path.split(".")[0]
        if self.get(f"{file_endpoint}.json"):
            return True
        return False

    def _is_dir_empty(self, path: str) -> bool:
        """Checks if the folder is empty."""
        if len(self.get(f"{path}.json").keys()) > 3:
            return False
        return True

    def _get_parent_dir(self, path: str) -> str:
        """Get the parent directory"""
        return "/".join(path.split("/")[:-1])

    def top_level_parser(self, json_doc: dict) -> list:
        """Parses files and directories at depth level of 1

        Args:
            json_doc (dict): JSON response with file and directories for a path

        Returns:
            list: List of files and directories at depth=1
        """
        parsed_list = []
        for key, value in json_doc.items():
            if type(value) is dict:
                parsed_list.append(
                    value["name"] if value["type"] == "FILE" else f"/{value['name']}"
                )
        return parsed_list

    def fs_parser(self, obj: dict) -> dict:
        """Parses the file system JSON response removing the metadata in the skeleton format

        Args:
            obj (dict): JSON response to be parsed

        Returns:
            dict: Parsed dictionary
        """
        res = {}
        for elem in obj:
            if type(obj[elem]) is dict:
                if obj[elem]["type"] == "DIR":
                    dir_key = f"{obj[elem]['name']}"
                    res[dir_key] = [self.fs_parser(obj[elem])]
                if obj[elem]["type"] == "FILE":
                    res[dir_key].append(f"<{obj[elem]['name']}/>")
        return res

    def ls(self, path: str):
        """List all files and directory for a given path

        Args:
            path (str): Directory or file path
        """
        try:
            if path == "/":
                res = self.get("/.json")
            else:
                res = self.get(f"{path}.json")
            print("\t\t".join(self.top_level_parser(res)))
        except Exception as e:
            print("Invalid Path:", path)

    def mkdir(self, path: str):
        """Create a new directory only if parent directory exists. This function will also
        check for user directory, if not found it will create one for the user.

        Args:
            path (str): Directory Path
        """
        try:
            assert path.startswith("/"), "Path must start with /"

            user_dir = "/".join(path.split("/")[:2])
            # Check and create user directory
            if len(path.split("/")) == 3:
                if not self._dir_exists(user_dir):
                    print("User dir not found. Creating one...")
                    self.mkdir(user_dir)

            # Check if parent directory exists
            if len(path.split("/")) >= 3:
                is_parent = self._dir_exists(self._get_parent_dir(path))
                if is_parent:
                    ...
                else:
                    print("Invalid Path:", path)
                    return

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

    def rmdir(self, path: str):
        """Remove directory only if empty

        Args:
            path (str): Directory Path
        """
        try:
            if self._is_dir_empty(path):
                self.delete(f"{path}.json")
                print(f"Successfully deleted directory: {path}")
            else:
                print(f"Directory is not empty: {path}")
        except Exception as e:
            print(f"Invalid path: {path}")

    def create(self, path: str):
        """Create/Write to new file

        Args:
            path (str): File path
        """
        try:
            if "/" not in path:
                if not self._file_exists(f"/{path}"):
                    self.put(
                        f"/{path.split('.')[0]}",
                        data={
                            "type": "FILE",
                            "name": path.split("/")[-1],
                            "id": uuid.uuid4().hex,
                            "content": "hello world",
                        },
                    )
                    print(f"Successfully created file: {path}")
            elif self._dir_exists(self._get_parent_dir(path)):
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
        """Delete file

        Args:
            path (str): File Path
        """
        path = f"/{path}" if "/" not in path else path
        file_endpoint = path.split(".")[0]
        if self._file_exists(path):
            self.delete(f"{file_endpoint}.json")
            print(f"Successfully deleted file: {path}")
        else:
            print(f"Invalid Path: {path}")

    def export(self, output_path: str = "fs_output.xml"):
        """Export the file system skeleton after parsing as XML file

        Args:
            output_path (str): Save to location. Eg: "output.xml"
        """
        assert ".xml" in output_path, "Can only write to xml files"
        try:
            res = self.fs_parser(self.get("/.json"))
            res = dict2xml({"root": res})
            res = "\n".join([line for line in res.split("\n") if line.strip() != ""])

            with open(output_path, "w") as f:
                f.write(res)
        except Exception as e:
            print(e)

    def execute(self):
        """Execute the input command"""
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
            res.append(f"{line_padding}</{tag_name}>")
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
    OR
    python3 edfs.py <command> <action-item>

    Examples:
    1.  python edfs.py -mkdir /kayvan/test
        python edfs.py -mkdir /test-user

    2.  python edfs.py -create /kayvan/test.txt

    3.  python edfs.py -rmdir /test-user

    4.  python edfs.py -ls /
        python edfs.py -ls /test

    5.  python edfs.py -rm /kayvan/test.txt

    6.  python edfs.py -export output.xml
    """
    args = parse_args(sys.argv)
    fs = HDFSEmulator(args["command"], args["action_item"])
    fs.execute()
