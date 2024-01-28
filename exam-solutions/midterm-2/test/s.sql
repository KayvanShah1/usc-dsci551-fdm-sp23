SELECT buyer
FROM Purchase
GROUP BY buyer
HAVING COUNT(*) = (
        SELECT MIN(count_products)
        FROM (
                SELECT COUNT(*) as count_products
                FROM Purchase
                GROUP BY buyer
            ) sub
    );