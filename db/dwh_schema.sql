CREATE TABLE stock_dim
(
	SK_stock_id SERIAL PRIMARY KEY,
	stock_id    VARCHAR(5) NOT NULL,
	company     VARCHAR(100) NOT NULL,
	category    VARCHAR(100) NOT NULL,
	price       DECIMAL(10, 2) NOT NULL
);

CREATE TABLE user_dim
(
    SK_user_id SERIAL PRIMARY KEY,
    user_id    INT NOT NULL,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(100) NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date   TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL
);

CREATE TABLE transaction_fact
(
    transaction_id      SERIAL PRIMARY KEY,
    FK_user_id          INT NULL,
    FK_stock_id         INT NOT NULL,
    quantity         	INT NOT NULL,
    transaction_date    TIMESTAMP,
    CONSTRAINT transaction_stock_stock_id_fk FOREIGN KEY (FK_stock_id) REFERENCES
    stock_dim (SK_stock_id),
    CONSTRAINT transaction_user_user_id_fk FOREIGN KEY (FK_user_id) REFERENCES
    user_dim (SK_user_id)
);
