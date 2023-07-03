CREATE DATABASE stock_db;

CREATE TABLE stock
  (
     stock_id VARCHAR(5) NOT NULL PRIMARY KEY,
     company  VARCHAR(100) NOT NULL,
     category VARCHAR(100) NOT NULL,
     price    DECIMAL(10, 2) NOT NULL,
     CONSTRAINT company UNIQUE (company)
  );

CREATE TABLE user
  (
     user_id    INT auto_increment PRIMARY KEY,
     name       VARCHAR(100) NOT NULL,
     email      VARCHAR(100) NOT NULL,
     updated_at TIMESTAMP,
     status     VARCHAR(1) DEFAULT NULL
  );

CREATE TABLE transaction
  (
     transaction_id   INT auto_increment PRIMARY KEY,
     user_id          INT NULL,
     stock_id         VARCHAR(5) NOT NULL,
     quantity         INT NOT NULL,
     transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
     updated_at       TIMESTAMP,
     status           VARCHAR(1) DEFAULT NULL,
     CONSTRAINT transaction_stock_stock_id_fk FOREIGN KEY (stock_id) REFERENCES
     stock (stock_id),
     CONSTRAINT transaction_user_user_id_fk FOREIGN KEY (user_id) REFERENCES
     user (user_id)
  );

CREATE INDEX stock_id ON transaction (stock_id);

CREATE INDEX user_id ON transaction (user_id);
