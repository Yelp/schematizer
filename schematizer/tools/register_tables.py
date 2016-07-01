# -*- coding: utf-8 -*-
""" This module spins up a test schematizer container and registers all the
mysql tables against the schematizer container. It picks up the database
credentials from the topology file and verifies if the returned schema complies
with the input table. Displays the number of successfully registered tables and
failed tables.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import getpass
import json
import optparse
import subprocess
import sys
from contextlib import contextmanager

import pymysql
import requests
from docker import Client
from yelp_conn.topology import TopologyFile


PROJECT = "testschematizer" + getpass.getuser()
SERVICE = "schematizerservice"


class ContainerUnavailableError(Exception):
    def __init__(self, project='unknown', service='unknown'):
        Exception.__init__(
            self,
            "Container for project {0} and service {1} failed to start".format(
                project, service
            )
        )


def get_arguments():
    """ Parse the command-line options found in sys.argv[1:] and returns a pair
        (values, args) where 'values' is an Values instance (with all your
        option values) and 'args' is the list of arguments left over after
        parsing options.
    """
    args = sys.argv[1:]

    op = optparse.OptionParser(
        usage='%prog [options]',
        description="Register all the tables of a database against a test "
        "Schematizer container."
    )
    op.add_option(
        '--cluster_name',
        '-c',
        default='primary',
        help='Name of the cluster to connect to. Default is "%default"'
    )
    op.add_option(
        '--proddb',
        '-p',
        action='store_true',
        default=False,
        help='Use topology for proddbs'
    )
    return op.parse_args(args)


def get_container_ip_address(project, service):
    """ Returns container IP address of the first container matching project
    and service.
    Raises ContainerUnavailableError if the container is unavailable.

    Args:
        project: Name of the project the container is hosting
        service: Name of the service that the container is hosting
    """
    docker_client = Client(version='auto')
    try:
        for container in docker_client.containers():
            if container['Labels'].get(
                'com.docker.compose.project'
            ) == project and container['Labels'].get(
                'com.docker.compose.service'
            ) == service:
                return container[
                    'NetworkSettings'
                ]['Networks']['bridge']['IPAddress']
    except:
        raise ContainerUnavailableError(project=project, service=service)


def _execute_query(connection, query):
    """ Executes the query and returns the result """
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        connection.commit()
        return results


def get_mysql_tables_info(conn):
    """ Fetches create table statements and columns of all the tables and
    returns a table_to_info_map with table names as keys and tuples of create
    table statement and list of columns as values.
    """
    table_to_info_map = {}
    table_entries = _execute_query(conn, query='show tables;')
    for entry in table_entries:
        table_name = entry[0]
        results = _execute_query(
            conn,
            query='show create table {};'.format(table_name)
        )
        _, create_tbl_stmt = results[0]
        create_tbl_stmt = create_tbl_stmt.replace('\n', '')
        results = _execute_query(
            conn,
            query='show columns from {};'.format(table_name)
        )
        columns = [column[0] for column in results]
        table_to_info_map[table_name] = (create_tbl_stmt, columns)
    return table_to_info_map


@contextmanager
def setup_schematizer_container():
    """ Set up a scheamtizer container and yields the IP address of the host
    container.
    Note: Removes the container when exiting the context manager.
    """
    try:
        run_docker_compose_command('up', '-d', SERVICE)
        host = get_container_ip_address(PROJECT, SERVICE)
        yield host
    finally:
        run_docker_compose_command('kill')
        run_docker_compose_command('rm', '--force')


def run_docker_compose_command(*args):
    """ Executes docker compose command with the given arguments """
    subprocess.call(
        ['docker-compose', '--project-name={}'.format(PROJECT)] + list(args),
        stderr=subprocess.STDOUT
    )


def get_connection_param_from_topology(topology_file, cluster):
    """ Reads the given topology file and returns the first element in the
    connection params for the given cluster replica ('slave') pair. Throws
    exception if the given cluster, replica pair is not part of this toplogy
    file """
    topology = TopologyFile.new_from_file(topology_file)
    return topology.get_first_connection_param(cluster, 'slave')


def register_mysql_tables(cluster, mysql_tables, curler, host):
    """ Registers the given mysql tables against the Schematizer container. """
    payload = {
        'namespace': 'schematizer_test_for_{}'.format(cluster),
        'source': None,
        'source_owner_email': 'test_schematizer@yelp.com',
        'contains_pii': False,
        'new_create_table_stmt': None
    }
    table_to_avro_fields_map = {}
    for table_name, table_info in mysql_tables.iteritems():
        payload['source'] = table_name
        payload['new_create_table_stmt'] = table_info[0]
        fields = curler(host=host, post_payload=payload)
        table_to_avro_fields_map[table_name] = fields
    return table_to_avro_fields_map


def post_to_schematizer(host, post_payload):
    uri = 'http://{}:8888/v1/schemas/mysql'.format(host)
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    response = requests.post(
        uri,
        data=json.dumps(post_payload),
        headers=headers
    )
    if response.status_code == 200:
        response_json = json.loads(response.json()['schema'])
        fields = response_json['fields']
        return [field['name'] for field in fields]
    return []


def verify_mysql_table_to_avro_schema(
    table_to_mysql_columns_map,
    table_to_schema_fields_map
):
    """ Compares the mysql_tables columns with the avro schema columns and
    returns a tuple with number of successfully registed tables and
    unregistered tables as values.
    """
    registered_tables = []
    error_tables = []
    for table_name, table_info in table_to_mysql_columns_map.iteritems():
        _, columns = table_info
        output_columns = table_to_schema_fields_map.get(table_name)
        if (set(columns) == set(output_columns) and
                len(columns) == len(output_columns)):
            registered_tables.append(table_name)
        else:
            error_tables.append(table_name)

    return registered_tables, error_tables


@contextmanager
def setup_connection(connection_param):
    """ Connect to a MySQL database with the given connection parameters and
    yields the connection.
    Note: Closes the connection when exiting the context manager.
    """
    connection = None
    try:
        connection = pymysql.connect(
            host=connection_param['host'],
            user=connection_param['user'],
            password=connection_param['passwd'],
            db=connection_param['db'],
            port=connection_param['port'],
            charset=connection_param['charset']
        )
        yield connection
    finally:
        if connection:
            connection.close()


def get_topology_file(is_proddb):
    return (
        '/nail/srv/configs/topology-proddb.yaml'
        if is_proddb else '/nail/srv/configs/topology.yaml'
    )


def main():
    options, _ = get_arguments()
    topology_file = get_topology_file(options.proddb)
    conn_param = get_connection_param_from_topology(
        topology_file,
        options.cluster_name
    )

    with setup_connection(conn_param) as conn:
        table_to_info_map = get_mysql_tables_info(conn)

    with setup_schematizer_container() as host:
        table_to_avro_fields_map = register_mysql_tables(
            options.cluster_name,
            table_to_info_map,
            post_to_schematizer,
            host
        )
    registered_tables, error_tables = verify_mysql_table_to_avro_schema(
        table_to_info_map,
        table_to_avro_fields_map
    )
    print ("Schematizer successfully processed " +
           str(len(registered_tables)) + " tables.")
    print ("Schematizer failed for " + str(len(error_tables)) +
           " tables which are: " + str(error_tables))


if __name__ == "__main__":
    main()
