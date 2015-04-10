#!/bin/bash
# This script comes from the base image. It starts mysqld in the background
bash /opt/startup.sh

# Create our database and users
cat setup.sql | mysql

# Create tables
cat tables/*.sql | mysql yelp_schematizer

# You could also add db fixtures here if you want them

# Shutdown mysqld
mysqladmin shutdown