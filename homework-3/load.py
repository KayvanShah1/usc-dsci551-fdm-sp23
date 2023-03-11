import sys

from lxml import etree

from sqlalchemy import create_engine
from sqlalchemy.orm import (
    sessionmaker,
    relationship,
    DeclarativeBase,
    Mapped,
    mapped_column,
)
from sqlalchemy import Integer, String, ForeignKey, BigInteger, SmallInteger


# Data Models
class Base(DeclarativeBase):
    pass


class INode(Base):
    __tablename__ = "inode"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    type: Mapped[str] = mapped_column(String(10), nullable=False)
    name: Mapped[str] = mapped_column(String(30), nullable=False)
    replication: Mapped[int] = mapped_column(SmallInteger(30), default=1)
    mtime: Mapped[int] = mapped_column(BigInteger)
    atime: Mapped[int] = mapped_column(BigInteger)
    preferredBlockSize: Mapped[int] = mapped_column(Integer, default=134217728)
    permission: Mapped[str] = mapped_column(String(120))

    blocks: Mapped["Blocks"] = relationship(back_populates="blocks")
    directory: Mapped["Directory"] = relationship(back_populates="directory")

    def __repr__(self) -> str:
        return f"INode(id={self.id!r}, name={self.name!r}, type={self.type!r})"


class Blocks(Base):
    __tablename__ = "blocks"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, index=True, doc="inumber of block"
    )
    inumber: Mapped[str] = mapped_column(
        String, ForeignKey("inode.id"), doc="inode id of file"
    )
    genstamp: Mapped[str] = mapped_column(Integer)
    numBytes: Mapped[int] = mapped_column(Integer, default=1)

    inode: Mapped["INode"] = relationship(
        back_populates="inode", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"Blocks(id={self.id!r}, inumber={self.inumber!r}, numBytes={self.numBytes!r})"


class Directory(Base):
    __tablename__ = "directory"

    parent: Mapped[int] = mapped_column(
        Integer, ForeignKey("inode.id"), doc="inumber of parent dir"
    )
    child: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("inode.id"),
        doc="inumber of file or directory under parent dir",
    )

    inode: Mapped["INode"] = relationship(
        back_populates="inode", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"Directory(parent={self.parent!r}, child={self.child!r}"


class MySQLDBConfig:
    """Firebase Realtime DB config"""

    user = "dsci551"
    password = "Dsci-551"
    db_name = "dsci551"

    connection_str = f"mysql+pymysql:///{user}:{password}@localhost/{db_name}"


class MySQLClient:
    def __init__(self, create_tables=False):
        self.engine = create_engine(
            MySQLDBConfig.connection_str,
            connect_args={"check_same_thread": False},
            echo=True,
        )
        if create_tables:
            Base.metadata.create_all(self.engine, checkfirst=True)
        self.db = self.get_db()

    def get_db(self):
        db = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
        )
        try:
            yield db
        finally:
            db.close()

    def create_obj(self, obj):
        self.db.add(obj)
        self.db.commit()
        self.db.refresh(obj)
        print(obj.__repr__())


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

            inode_obj = INode(
                id=inode_id,
                type=type_,
                name=name,
                replication=replication,
                mtime=mtime,
                atime=atime,
                permission=permission,
                preferredBlockSize=preferredBlockSize,
            )
            self.create_obj(inode_obj)

            blocks = tag.xpath("./blocks/block")
            if len(blocks) != 0:
                for block in blocks:
                    block_id = self.parse_int(block.xpath("./id")[0])
                    genstamp = self.parse_int(block.xpath("./genstamp")[0])
                    numBytes = self.parse_int(block.xpath("./numBytes")[0])

                    blocks_obj = Blocks(
                        id=block_id,
                        inumber=inode_id,
                        numBytes=numBytes,
                        genstamp=genstamp,
                    )
                    self.create_obj(blocks_obj)

    def load_directory_table(self):
        for tag in self.tree.xpath("//INodeDirectorySection/directory"):
            parent = self.parse_int(tag.find("parent"))
            children = tag.findall("child")
            for child in children:
                child = self.parse_int(child.text)
                directory_obj = Directory(parent=parent, child=child)
                self.create_obj(directory_obj)

    def load(self):
        self.load_inodes_blocks_table()
        self.load_directory_table()


def parse_args(line: list[str]) -> dict:
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
