-- Drop and create inode table
DROP TABLE IF EXISTS `inode`;

CREATE TABLE IF NOT EXISTS `inode`
(
    id INT OPTIONS (description = "inumber"),
    type CHAR (10) NOT NULL,
    name CHAR (30) NOT NULL UNIQUE,
    replication TINYINT DEFAULT 1,
    mtime BIGINT, 
    atime BIGINT,
    preferredBlockSize INT DEFAULT 134217728,  
    permission CHAR (120),
    PRIMARY KEY (id)
);

-- Drop and create blocks table
DROP TABLE IF EXISTS `blocks`;

CREATE TABLE IF NOT EXISTS `blocks`
(
    id INT OPTIONS (description = "inumber of block"),
    inumber INT (10) OPTIONS (description = "inode id of file"),
    genstamp INT,
    numBytes INT NOT NULL,
    PRIMARY KEY (id),
    FORIEGN KEY (inumber) REFERENCES inode(id) ON DELETE CASCADE ON UPDATE NO ACTION
);

-- Drop and create directory table
DROP TABLE IF EXISTS `directory`;

CREATE TABLE IF NOT EXISTS `directory`
(
    parent INT OPTIONS (description = "inumber of parent dir"),
    child INT OPTIONS (description = "inumber of file or directory under parent dir"),
    FORIEGN KEY (parent) REFERENCES inode(id) ON DELETE CASCADE ON UPDATE NO ACTION,
    FORIEGN KEY (child) REFERENCES inode(id) ON DELETE CASCADE ON UPDATE NO ACTION
);