-- Question 1
SELECT name
FROM inode
WHERE type = "FILE"
    AND mtime = (
        SELECT max(mtime)
        FROM inode
        WHERE type = "FILE"
    );
-- Question 2
SELECT name
FROM inode
WHERE permission LIKE "%6%";
-- Question 3
SELECT i.id,
    i.name,
    b.numBytes
from inode as i,
    blocks as b
where type = "FILE"
    and i.id = b.inumber;
-- Question 4
SELECT i.id,
    i.name,
    count(d.child) as num_obj
from inode as i
    JOIN directory d ON i.id = d.parent
GROUP BY i.id,
    i.name;
-- Question 5
SELECT i.name
from inode as i
    JOIN directory d ON i.id = d.parent
GROUP BY i.name,
    i.id
HAVING count(d.child) >= 2;