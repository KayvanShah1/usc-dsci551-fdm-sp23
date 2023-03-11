import sys
import pymysql

from lxml import etree


class MySQLDBConfig:
    """Firebase Realtime DB config"""

    user = "dsci551"
    password = "Dsci-551"
    db_name = "dsci551"


class MySQLClient:
    def __init__(self):
        self.connection = pymysql.connect(
            host="localhost",
            user=MySQLDBConfig.user,
            password=MySQLDBConfig.password,
            db=MySQLDBConfig.db_name,
        )
        self.cursor = self.connection.cursor()

    def create_inode(
        self, id, type, name, replication, mtime, atime, permission, preferredBlockSize
    ):
        sql = "insert into inode values (%s,%s,%s,%s,%s,%s,%s,%s)"
        resp = self.cursor.execute(
            sql,
            (id, type, name, replication, mtime, atime, preferredBlockSize, permission),
        )
        print("Number of rows affected:", resp)

    def create_block(self, id, inumber, numBytes, genstamp):
        sql = "insert into blocks values (%s,%s,%s,%s)"
        resp = self.cursor.execute(
            sql,
            (id, inumber, genstamp, numBytes),
        )
        print("Number of rows affected:", resp)

    def create_dir(self, parent, child):
        sql = "insert into directory values (%s,%s)"
        resp = self.cursor.execute(
            sql,
            (parent, child),
        )
        print("Number of rows affected:", resp)


class LoadDatabase(MySQLClient):
    def __init__(self, fs_xml_path: str = None):
        super().__init__()
        self.fs_xml_path = fs_xml_path
        self.tree = self.get_tree(self.fs_xml_path)

    @staticmethod
    def get_tree(path):
        with open(path) as f:
            return etree.parse(f)

    @staticmethod
    def parse_int(element):
        if element is not None and element.text is not None:
            return int(element.text)
        return 0

    @staticmethod
    def parse_str(element):
        if element is not None and element.text is not None:
            return element.text.strip()
        return ""

    def load_inodes_blocks_table(self):
        for tag in self.tree.xpath("//INodeSection/inode"):
            inode_id = self.parse_int(tag.find("id"))
            type_ = self.parse_str(tag.find("type"))
            name = self.parse_str(tag.find("name"))
            replication = self.parse_int(tag.find("replication"))
            mtime = self.parse_int(tag.find("mtime"))
            atime = self.parse_int(tag.find("atime"))
            preferredBlockSize = self.parse_int(tag.find("preferredBlockSize"))
            permission = self.parse_str(tag.find("permission"))

            self.create_inode(
                id=inode_id,
                type=type_,
                name=name,
                replication=replication,
                mtime=mtime,
                atime=atime,
                permission=permission,
                preferredBlockSize=preferredBlockSize,
            )

            blocks = tag.xpath("./blocks/block")
            if len(blocks) != 0:
                for block in blocks:
                    block_id = self.parse_int(block.xpath("./id")[0])
                    genstamp = self.parse_int(block.xpath("./genstamp")[0])
                    numBytes = self.parse_int(block.xpath("./numBytes")[0])

                    self.create_block(
                        id=block_id,
                        inumber=inode_id,
                        numBytes=numBytes,
                        genstamp=genstamp,
                    )

    def load_directory_table(self):
        for tag in self.tree.xpath("//INodeDirectorySection/directory"):
            parent = self.parse_int(tag.find("parent"))
            children = tag.findall("child")
            for child in children:
                child = int(child.text)
                self.create_dir(parent=parent, child=child)

    def load(self):
        self.load_inodes_blocks_table()
        self.load_directory_table()


def parse_args(line: list) -> dict:
    """Parse the command line arguments

    Args:
        line (str): Input command line

    Returns:
        dict: Arguments dictionary
    """
    args = {"file": line[0], "fs_xml_path": line[1]}
    return args


if __name__ == "__main__":
    """To run the file execute the command
    python load.py <fsimage.xml>
    OR
    python3 load.py <fsimage.xml>
    """
    if len(sys.argv) < 2:
        print("Usage: python3 load.py <fsimage.xml>")
        sys.exit(1)

    args = parse_args(sys.argv)
    fs = LoadDatabase(args["fs_xml_path"])
    fs.load()
