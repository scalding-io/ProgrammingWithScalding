-- To create the test database:
CREATE DATABASE testdb;
USE testdb
CREATE TABLE tableName
(
user VARCHAR(16),
time TIMESTAMP,
activity VARCHAR(256),
code SMALLINT
);
SELECT * from tableName;