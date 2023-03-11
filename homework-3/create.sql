-- Create a new database
CREATE DATABASE IF NOT EXISTS dsci551;

-- Drop tables
DROP TABLE IF EXISTS `directory`;
DROP TABLE IF EXISTS `blocks`;
DROP TABLE IF EXISTS `inode`;

-- Create Tables
CREATE TABLE IF NOT EXISTS `inode`
(
    id INT COMMENT "inumber",
    type CHAR (10) NOT NULL,
    name CHAR (30) NOT NULL,
    replication TINYINT DEFAULT 1,
    mtime BIGINT, 
    atime BIGINT,
    preferredBlockSize INT DEFAULT 134217728,  
    permission CHAR (120),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS `blocks`
(
    id INT COMMENT "inumber of block",
    inumber INT (10) COMMENT "inode id of file",
    genstamp INT,
    numBytes INT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (inumber) REFERENCES inode(id) ON DELETE CASCADE ON UPDATE NO ACTION
);

CREATE TABLE IF NOT EXISTS `directory`
(
    parent INT COMMENT "inumber of parent dir",
    child INT COMMENT "inumber of file or directory under parent dir",
    FOREIGN KEY (parent) REFERENCES inode(id) ON DELETE CASCADE ON UPDATE NO ACTION,
    FOREIGN KEY (child) REFERENCES inode(id) ON DELETE CASCADE ON UPDATE NO ACTION
);