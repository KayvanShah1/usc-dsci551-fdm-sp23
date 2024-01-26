CREATE TABLE Person (
    name VARCHAR(20) PRIMARY KEY,
    city VARCHAR(50),
    phone VARCHAR(20)
);
CREATE TABLE Product (
    name VARCHAR(20) PRIMARY KEY,
    maker VARCHAR(50),
    category VARCHAR(50)
);
CREATE TABLE Purchase (
    buyer VARCHAR(20),
    seller VARCHAR(20),
    product VARCHAR(50),
    store VARCHAR(50),
    price DECIMAL(10, 2),
    PRIMARY KEY (buyer, seller, product),
    FOREIGN KEY (buyer) REFERENCES Person (name) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (seller) REFERENCES Person (name) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (product) REFERENCES Product (name) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE Laptop (
    name VARCHAR(20) PRIMARY KEY,
    OS VARCHAR(50),
    FOREIGN KEY (name) REFERENCES Product (name) ON DELETE CASCADE ON UPDATE CASCADE
)