CREATE DATABASE stock_db;

CREATE TABLE Stock
  (
     stock_id VARCHAR(5) NOT NULL PRIMARY KEY,
     company  VARCHAR(100) NOT NULL,
     category VARCHAR(100) NOT NULL,
     price    DECIMAL(10, 2) NOT NULL,
     CONSTRAINT company UNIQUE (company)
  );

CREATE TABLE User
  (
     user_id    INT auto_increment PRIMARY KEY,
     name       VARCHAR(100) NOT NULL,
     email      VARCHAR(100) NOT NULL,
     updated_at TIMESTAMP,
     status     VARCHAR(1) DEFAULT NULL
  );

CREATE TABLE Transaction
  (
     transaction_id   INT auto_increment PRIMARY KEY,
     user_id          INT NULL,
     stock_id         VARCHAR(5) NOT NULL,
     quantity         INT NOT NULL,
     transaction_date TIMESTAMP,
     CONSTRAINT transaction_stock_stock_id_fk FOREIGN KEY (stock_id) REFERENCES
     Stock (stock_id),
     CONSTRAINT transaction_user_user_id_fk FOREIGN KEY (user_id) REFERENCES
     User (user_id)
  );

CREATE INDEX stock_id ON Transaction (stock_id);

CREATE INDEX user_id ON Transaction (user_id);
