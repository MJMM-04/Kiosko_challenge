create database Kiosko;

create table fact_stockshare (
date date not null, 
open float not null, 
close float not null,
high float not null,
low float not null,
nolumen bigint not null,
name_id int not null,
created_at timestamp not null, 
deleted_at timestamp); 



create table dim_name (
name_Id  serial,
name Varchar(10) not null,
created_at timestamp not null, 
updated_at timestamp, 
deleted_at timestamp);