create database stock_db;

create table Stock
(
    stock_id varchar(5)     not null
        primary key,
    company  varchar(100)   not null,
    category varchar(100)   not null,
    price    decimal(10, 2) not null,
    constraint company
        unique (company)
);

create table User
(
    user_id int auto_increment
        primary key,
    name    varchar(100) not null,
    email   varchar(100) not null
);

create table Transaction
(
    transaction_id   int auto_increment
        primary key,
    user_id          int                                 null,
    stock_id         varchar(5)                          not null,
    quantity         int                                 not null,
    transaction_date timestamp default CURRENT_TIMESTAMP null,
    constraint Transaction_Stock_stock_id_fk
        foreign key (stock_id) references Stock (stock_id),
    constraint Transaction_User_user_id_fk
        foreign key (user_id) references User (user_id)
);

create index stock_id
    on Transaction (stock_id);

create index user_id
    on Transaction (user_id);
