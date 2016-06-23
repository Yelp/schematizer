# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import unicode_literals

import json
import optparse
import pymysql
import requests
import subprocess
import sys
import yaml

def _get_options(args):
    op = optparse.OptionParser(
            usage='%prog [options]',
            description="Register all the tables of a database against a test "
                        "Schematizer container.")
    op.add_option('--cluster_name', '-c',
                  default='primary',
                  help='Name of the cluster to connect '
                       'to. Default is "%default"')
    op.add_option('--config_file', '-f',
                  default='/nail/srv/configs/topology-proddb.yaml',
                  help='Path of the config file containing db information. '
                       'Default is "%default"')
    op.add_option('--db_name', '-d',
                  help='Name of the database in the cluster to '
                       'connect to. DB name from the config_file will be picked '
                       'up if not specified.')
    op.add_option('--username', '-u',
                  help='username to connect to mysql as. Username will be '
                       'picked up from the config file if not specified.')
    op.add_option('--password', '-p',
                  help='password for the username to connect to mysql. Password '
                       'will be picked up from the config file if not specified.')
    return op.parse_args(args)


def _get_db_connection(host_ipaddress, db_name, db_user, db_password):
    return pymysql.connect(
        host=host_ipaddress,
        user=db_user,
        password=db_password,
        db=db_name,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def _execute_query_get_all_rows(connection, query):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            connection.commit()
            return results
    except:
        connection.close()


def _get_column_maps(tables, cluster_ipaddress, db_name, username, password):
    connection = _get_db_connection(
        cluster_ipaddress,
        db_name,
        username,
        password
    )
    table_to_columns_map = {}
    table_to_create_schema_stmt_map = {}
    for table_name in tables:
        query = "show create table " + db_name + "." + table_name + ";"
        table_create_stmt = _execute_query_get_all_rows(connection, query)
        table_create_stmt[0]['Create Table'] = table_create_stmt[0][
            'Create Table'
        ].replace('\n','')
        table_to_create_schema_stmt_map[table_create_stmt[0]['Table']] = \
            table_create_stmt[0]['Create Table']
        query = "show columns from yelp." + table_name + ";"
        table_columns = _execute_query_get_all_rows(connection, query)
        column_names = set()
        for column in table_columns:
                column_names.add(column['Field'])
        table_to_columns_map[table_name] = column_names
    connection.close()
    return (table_to_create_schema_stmt_map, table_to_columns_map)


def _get_table_names(cluster_ipaddress, db_name, username, password):
    tables = []
    query = "show tables;"
    connection = _get_db_connection(
        cluster_ipaddress,
        db_name,
        username,
        password
    )
    result = _execute_query_get_all_rows(connection, query)
    connection.close()
    for row in result:
        tables.append(row['Tables_in_yelp'])
    return tables


def _set_up_schematizer_container():
    print "Setting up Schematizer container..."
    db_container_name = "db_test_container"
    configs_container_name = "configs_test_container"
    schematizer_container_name = "schematizer_test_container"

    db_container_not_found = subprocess.call(
        "docker ps -a | grep " + db_container_name,
        shell=True
    )
    if db_container_not_found:
        subprocess.call(
            "docker run -d --name " + db_container_name +
            " docker-dev.yelpcorp.com/schematizer_database",
            shell=True
        )

    configs_container_not_found = subprocess.call(
        "docker ps -a | grep " + configs_container_name,
        shell=True
    )
    if configs_container_not_found:
        subprocess.call(
            "docker run -d --name " + configs_container_name +
            " docker-dev.yelpcorp.com/schematizer_configs",
            shell=True
        )

    schematizer_container_not_found = subprocess.call(
        "docker ps -a | grep " + schematizer_container_name,
        shell=True
    )
    if schematizer_container_not_found:
        subprocess.call(
            "docker run -d --name " + schematizer_container_name +
            " --link " + db_container_name + ":schematizerdatabase --volumes-from="
            + configs_container_name + " docker-dev.yelpcorp.com/schematizer_service "
            "/code/virtualenv_run/bin/python /code/serviceinit.d/schematizer "
            "start-dev", shell=True
        )

    schematizer_ipaddresss = subprocess.check_output(
        "docker inspect -f '{{ .NetworkSettings.IPAddress }}' " +
        schematizer_container_name, shell=True
    )
    schematizer_ipaddresss = schematizer_ipaddresss.rstrip()
    if schematizer_ipaddresss:
        print "Schematizer container setup completed successfully."
    else:
        print "Error while setting up schematizer container.Exiting..."
        sys.exit(2)
    return schematizer_ipaddresss


if __name__ == "__main__":
    db_args = sys.argv[1:]
    options, args = _get_options(db_args)
    yaml_file = open(options.config_file)
    config = yaml.load(yaml_file)['topology']
    host_ipaddress = ''
    user = ''
    pwd = ''
    db_name = ''

    for entry_obj in config:
        if options.cluster_name == entry_obj['cluster']:
            entry = entry_obj['entries'][0]
            host_ipaddress = entry['host']
            db_name = options.db_name if options.db_name else entry['db']
            user = options.username if options.username else entry['user']
            pwd = options.password if options.password else  entry['passwd']
            break;
    else:
        print 'Please enter a valid cluster_name. Exiting...'
        sys.exit(2)
    schematizer_ipaddress = _set_up_schematizer_container()

    print 'Executing script...'
    tables = _get_table_names(host_ipaddress, db_name, user, pwd)
    table_to_create_schema_stmt_map, table_to_columns_map = _get_column_maps(tables, host_ipaddress, db_name, user, pwd)

    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    url = 'http://' + schematizer_ipaddress + ':8888/v1/schemas/mysql'
    table_to_schema_id_map = {}
    error_tables = []

    for table_name, create_schema_stmt in table_to_create_schema_stmt_map.iteritems():
        payload = (
            '{"namespace": "test_namespace", '
            '"source_owner_email": "bam+batch@yelp.com", '
            '"source": "test_source", '
            '"contains_pii": true, '
            '"new_create_table_stmt": "' + create_schema_stmt + '"}'
        )
        response = requests.post(url, data=payload, headers=headers)
        if response.status_code == 200:
            response_json = json.loads(response.json()['schema'])
            columns = response_json['fields']
            column_names = set()
            for column in columns:
                column_names.add(column['name'])
            if table_to_columns_map[table_name] == column_names:
                table_to_schema_id_map[table_name]=response.json()['schema_id']
            else:
                error_tables.append(table_name)
        else:
            error_tables.append(table_name)

    print "Schematizer successfully processed " + str(len(table_to_schema_id_map)) + " tables."
    print "Schematizer failed for " + str(len(error_tables)) + " tables which are: " + str(error_tables)
