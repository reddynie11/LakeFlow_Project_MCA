-- =====================================================
-- SALES EAST DATA
-- =====================================================

CREATE OR REPLACE TABLE lakeflow_catalog.source.sales_east (
    sales_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    amount DECIMAL(10,2),
    sale_timestamp TIMESTAMP
);

-- Initial Load
INSERT INTO lakeflow_catalog.source.sales_east VALUES
(1, 101, 201, 2, 200.00, '2025-08-01 10:00:00'),
(2, 102, 202, 1, 120.00, '2025-08-01 10:05:00'),
(3, 103, 203, 5, 500.00, '2025-08-01 10:10:00'),
(4, 104, 204, 3, 330.00, '2025-08-01 10:15:00'),
(5, 105, 205, 4, 440.00, '2025-08-01 10:20:00');

-- =====================================================
-- SALES WEST DATA
-- =====================================================

CREATE OR REPLACE TABLE lakeflow_catalog.source.sales_west (
    sales_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    amount DECIMAL(10,2),
    sale_timestamp TIMESTAMP
);

-- Initial Load
INSERT INTO lakeflow_catalog.source.sales_west VALUES
(8, 107, 207, 1, 150.00, '2025-08-01 11:00:00'),
(9, 108, 208, 2, 260.00, '2025-08-01 11:05:00'),
(10, 109, 209, 3, 390.00, '2025-08-01 11:10:00'),
(11, 110, 210, 1, 130.00, '2025-08-01 11:15:00'),
(12, 111, 211, 4, 560.00, '2025-08-01 11:20:00');

-- =====================================================
-- PRODUCTS DATA
-- =====================================================

CREATE OR REPLACE TABLE lakeflow_catalog.source.products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    last_updated TIMESTAMP
);

-- Initial Load
INSERT INTO lakeflow_catalog.source.products VALUES
(201, 'Laptop', 'Electronics', 1000.00, '2025-07-31 12:00:00'),
(202, 'Phone', 'Electronics', 120.00, '2025-07-31 12:05:00'),
(203, 'Monitor', 'Electronics', 100.00, '2025-07-31 12:10:00'),
(204, 'Chair', 'Furniture', 110.00, '2025-07-31 12:15:00'),
(205, 'Desk', 'Furniture', 150.00, '2025-07-31 12:20:00'),
(206, 'Mouse', 'Electronics', 50.00, '2025-07-31 12:25:00'),
(207, 'Keyboard', 'Electronics', 60.00, '2025-07-31 12:30:00'),
(208, 'Lamp', 'Furniture', 130.00, '2025-07-31 12:35:00'),
(209, 'Router', 'Electronics', 130.00, '2025-07-31 12:40:00'),
(210, 'Table', 'Furniture', 130.00, '2025-07-31 12:45:00'),
(211, 'Notebook', 'Stationery', 140.00, '2025-07-31 12:50:00'),
(212, 'Pen', 'Stationery', 150.00, '2025-07-31 12:55:00');

-- =====================================================
-- CUSTOMERS DATA
-- =====================================================

CREATE OR REPLACE TABLE lakeflow_catalog.source.customers (
    customer_id INT,
    customer_name STRING,
    region STRING,
    last_updated TIMESTAMP
);

-- Initial Load
INSERT INTO lakeflow_catalog.source.customers VALUES
(101, 'Alice', 'East', '2025-07-31 13:00:00'),
(102, 'Bob', 'East', '2025-07-31 13:05:00'),
(103, 'Charlie', 'East', '2025-07-31 13:10:00'),
(104, 'Diana', 'East', '2025-07-31 13:15:00'),
(105, 'Ethan', 'East', '2025-07-31 13:20:00'),
(106, 'Fiona', 'East', '2025-07-31 13:25:00'),
(107, 'George', 'West', '2025-07-31 13:30:00'),
(108, 'Hannah', 'West', '2025-07-31 13:35:00'),
(109, 'Ian', 'West', '2025-07-31 13:40:00'),
(110, 'Jane', 'West', '2025-07-31 13:45:00'),
(111, 'Kevin', 'West', '2025-07-31 13:50:00'),
(112, 'Laura', 'West', '2025-07-31 13:55:00');
