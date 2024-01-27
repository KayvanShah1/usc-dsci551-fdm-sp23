CREATE VIEW LALaptop AS
SELECT Laptop.name, Laptop.OS
FROM Laptop
JOIN Product ON Laptop.name = Product.name
JOIN Purchase ON Product.name = Purchase.product
JOIN Person ON Purchase.buyer = Person.name
WHERE Person.city = 'LA';