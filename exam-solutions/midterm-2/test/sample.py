from pyspark.sql.functions import count, col

num_sold = (
    purchase.join(person, purchase.buyer == person.name)
    .filter(col("store") == "store1" and col("city") == "LA")
    .agg(count("*").alias("num_sold"))
)

num_sold.show()

person_names = personRDD.filter(lambda row: "123" in row.phone).map(
    lambda row: row.name
)
person_names.collect()

min_purchase = (
    purchaseRDD.map(lambda x: (x[0], 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[1], ascending=True)
    .first()
)
print(min_purchase[0])


store1_purchases = purchaseRDD.filter(lambda row: row.store == "store1")

store2_purchases = purchaseRDD.filter(lambda row: row.store == "store2")


buyers = set(store1_purchases.map(lambda row: row.buyer).distinct().collect()) & set(
    store2_purchases.map(lambda row: row.buyer).distinct().collect()
)
for buyer in buyers:
    print(buyer)


# filter purchases made in store1 or store2 and by people living in LA
filtered_purchases = purchaseRDD.filter(
    lambda x: (
        (x["store"] == "store1" or x["store"] == "store2")
        and x["buyer"]
        in personRDD.filter(lambda y: y["city"] == "LA")
        .map(lambda z: z["name"])
        .collect()
    )
)

# get the total count of products sold
product_count = filtered_purchases.count()

print(product_count)


# filter purchases made in store 1
store1_purchases = purchaseRDD.filter(lambda x: x["store"] == "store1")
joined_data = store1_purchases.join(productRDD, "product")

category_avg_price = (
    joined_data.map(lambda x: (x["category"], x["price"]))
    .groupByKey()
    .mapValues(lambda x: sum(x) / len(x))
)
for row in category_avg_price.collect():
    print(row[0], row[1])


product_RDD = productRDD.map(lambda row: (row["name"], row))
purchase_RDD = purchaseRDD.map(lambda row: (row["product"], row))
joined_RDD = purchase_RDD.join(product_RDD)

joined_RDD.filter(lambda row: row[1][0].store == 'store1')
.map(lambda row: (row[1][1].category, (row[1][0].price, 1)))
.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
.mapValues(lambda x: x[0] / x[1]).collect()