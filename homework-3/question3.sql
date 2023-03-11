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
FROM inode as i
    JOIN (
        SELECT parent,
            count(child) as num_files
        from (
                SELECT d.parent,
                    d.child
                FROM directory as d
                    JOIN inode i on i.id = d.child
                WHERE type = "FILE"
            ) as t
        GROUP BY parent
        HAVING num_files > 2
    ) as x ON i.id = x.parent;