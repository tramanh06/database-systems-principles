-- commands to run from terminal
-- cd to folder
-- mysql -uroot < create_tables.sql -t

use db_ass2;
drop table if exists R, S;

create table R
	(
		id int,
		name varchar(255)
	);

create table S
	(
		id int,
		name varchar(255)
	);

LOAD DATA INFILE "/Users/TramAnh/Dropbox/NTU/Year 4 Sem 1/CZ4031 Database System Principles/Project/Assignment 2/NetBean Project/RelR.txt"
INTO TABLE R 
FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n';

LOAD DATA INFILE "/Users/TramAnh/Dropbox/NTU/Year 4 Sem 1/CZ4031 Database System Principles/Project/Assignment 2/NetBean Project/RelS.txt"
INTO TABLE S 
FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n';
