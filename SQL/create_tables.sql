-- script to create table in MySQL

-- create database
-- create database dblp;
use dblp;

SET NAMES utf8mb4;

drop table if exists article, book, incollection, inproceedings, 
		publication, temp_authored;

create table article
	(
		pubid int,
		journal varchar(255),
		month varchar(255),
		volume varchar(255),
		num varchar(255) 
	);

create table book
	(
		pubid int,
		publisher varchar(255),
		isbn varchar(255)
	);

create table incollection
	(
		pubid int,
		publisher varchar(255),
		isbn varchar(255),
		booktitle varchar(255)
	);

create table inproceedings
	(
		pubid int,
		booktitle varchar(255)
	);

create table publication
	(
		pubid int,
		pubkey varchar(255),
		title varchar(1000),
		year int
	) DEFAULT CHARSET=utf8mb4;

create table temp_authored
	(
		pubid int,
		author varchar(255)
	);
-- Load csv file
-- SET @filepath = "/Users/TramAnh/Dropbox/NTU/Year 4 Sem 1/CZ4031 Database System Principles/Project/CZ4031 Database Principles/Data/";
-- SET @article_loc = CONCAT(@filepath, "article.csv");
LOAD DATA INFILE "/Users/TramAnh/Dropbox/NTU/Year 4 Sem 1/CZ4031 Database System Principles/Project/CZ4031 Database Principles/Data/article.csv"
INTO TABLE article 
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INFILE "/Users/TramAnh/Dropbox/NTU/Year 4 Sem 1/CZ4031 Database System Principles/Project/CZ4031 Database Principles/Data/book.csv"
INTO TABLE book 
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INFILE "/Users/TramAnh/Dropbox/NTU/Year 4 Sem 1/CZ4031 Database System Principles/Project/CZ4031 Database Principles/Data/incollection.csv"
INTO TABLE incollection 
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INFILE "/Users/TramAnh/Dropbox/NTU/Year 4 Sem 1/CZ4031 Database System Principles/Project/CZ4031 Database Principles/Data/inproceedings.csv"
INTO TABLE inproceedings 
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INFILE "/Users/TramAnh/Dropbox/NTU/Year 4 Sem 1/CZ4031 Database System Principles/Project/CZ4031 Database Principles/Data/publication.csv"
INTO TABLE publication 
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA INFILE "/Users/TramAnh/Dropbox/NTU/Year 4 Sem 1/CZ4031 Database System Principles/Project/CZ4031 Database Principles/Data/authored.csv"
INTO TABLE temp_authored 
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';






