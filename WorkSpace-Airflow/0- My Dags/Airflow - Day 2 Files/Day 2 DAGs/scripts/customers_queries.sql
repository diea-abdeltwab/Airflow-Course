-- ===================== Drop Table If Exists =====================
DROP TABLE IF EXISTS customers;

-- ===================== Create Table =====================
CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(50) PRIMARY KEY,
    customer_name   VARCHAR NOT NULL,
    address         VARCHAR NOT NULL,
    birth_date      DATE NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_DATE
);

-- ===================== Insert Initial Batch Data =====================
INSERT INTO customers (customer_id, customer_name, address, birth_date ,created_at) VALUES
('C001', 'Ahmed Ali', 'Cairo, Nasr City', '2001-05-12', CURRENT_DATE - 1),
('C002', 'Sara Mohamed', 'Giza, Dokki', '1999-11-23', CURRENT_DATE - 1),
('C003', 'Omar Hassan', 'Alexandria, Sidi Gaber', '2003-02-17', CURRENT_DATE - 1),
('C004', 'Mona Adel', 'Cairo, Maadi', '2000-07-30', CURRENT_DATE - 1),
('C005', 'Youssef Tarek', 'Giza, 6th of October', '2004-09-05', CURRENT_DATE - 1),
('C006', 'Nour Khaled', 'Mansoura, Downtown', '2002-12-01', CURRENT_DATE - 1),
('C007', 'Hana Samir', 'Cairo, Heliopolis', '1998-03-14', CURRENT_DATE - 1),
('C008', 'Karim Magdy', 'Tanta, Center', '2005-06-21', CURRENT_DATE - 1);

-- ===================== Insert Single Record (Default Created At) =====================
INSERT INTO customers (customer_id, customer_name, address, birth_date ,created_at) VALUES
('C009', 'Mohamed Ahmed', 'Cairo, Zamalek', '2000-01-01', DEFAULT);