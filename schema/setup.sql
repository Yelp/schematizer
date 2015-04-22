-- Setup database and users
-- Reference http://y/runbook-add-new-db

CREATE DATABASE yelp_schematizer DEFAULT CHARACTER SET utf8;

GRANT USAGE ON *.* TO 'schematizerdev'@'%';

GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, CREATE TEMPORARY TABLES
    ON `yelp_schematizer`.* TO 'schematizerdev'@'%' ;

GRANT USAGE ON *.* TO 'schematizerro'@'%';

GRANT SELECT ON `yelp_schematizer`.* TO 'schematizerro'@'%' ;