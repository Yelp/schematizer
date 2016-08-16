# -*- coding: utf-8 -*-
""" This module spins up a Schematizer container and registers all the
mysql tables against the container to test how many tables can be
successfully registered with the Schematizer. It picks up the database
credentials from the topology file and verifies if the returned schema complies
with the input table. Displays the number of successfully registered tables and
failed tables.
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import getpass
import subprocess
import time
from collections import namedtuple
from contextlib import contextmanager

import pymysql
import requests
import simplejson
from docker import Client
from yelp_conn.topology import TopologyFile


TableInfo = namedtuple('TableInfo', 'table_name, create_table_stmt columns')
RegisterResult = namedtuple('RegisterResult', ['success', 'result'])


def _setup_cli_options():
    parser = argparse.ArgumentParser(
        description="This script spins up a Schematizer container and "
                    "registers all the tables in the specified MySQL db. "
                    "It then outputs the registering outcome. The purpose of "
                    "this script is to manually verify whether Schematizer "
                    "supports the table schemas in any given MySQL database."
    )
    parser.add_argument(
        '--config-file',
        type=str,
        required=True,
        help='Path of the config file containing db topology information. '
             'Required.'
    )
    parser.add_argument(
        '-c',
        '--cluster-name',
        type=str,
        required=True,
        help='Name of the cluster to connect to, such as primary, aux, etc. '
             'Required.'
    )
    return parser


def run(parsed_args):
    conn_param = _get_connection_param_from_topology(
        parsed_args.config_file,
        parsed_args.cluster_name
    )
    with _setup_connection(conn_param) as conn:
        tables_info = _get_mysql_tables_info(conn)
    with _setup_schematizer_container() as host:
        register_tables_results = _register_tables(host, tables_info)
    results_stats = _verify_register_tables_results(register_tables_results)
    _output_results(results_stats)


def _get_connection_param_from_topology(topology_file, cluster):
    """ Reads the given topology file and returns the first element in the
    connection params for the given cluster replica ('slave') pair. Throws
    exception if the given cluster, replica pair is not part of this
    toplogy file """
    topology = TopologyFile.new_from_file(topology_file)
    return topology.get_first_connection_param(cluster, 'slave')


@contextmanager
def _setup_connection(connection_param):
    connection = None
    try:
        connect_params = {
            'user': connection_param['user'],
            'password': connection_param['passwd'],
            'db': connection_param['db'],
            'charset': connection_param['charset']
        }
        if connection_param.get('host'):
            connect_params['host'] = connection_param['host']
        if connection_param.get('port'):
            connect_params['port'] = connection_param['port']
        if connection_param.get('unix_socket'):
            connect_params['unix_socket'] = connection_param['unix_socket']
        connection = pymysql.connect(**connect_params)
        yield connection
    finally:
        if connection:
            connection.close()


def _get_mysql_tables_info(conn):
    """ Fetches create table statements and columns of all the tables.
    """
    table_entries = _execute_query(conn, query='show tables;')

    tables_info = []
    for entry in table_entries:
        table_name = entry[0]
        results = _execute_query(
            conn,
            query='show create table `{}`;'.format(table_name)
        )
        _, create_tbl_stmt = results[0]
        create_tbl_stmt = create_tbl_stmt.replace('\n', '')
        results = _execute_query(
            conn,
            query='show columns from `{}`;'.format(table_name)
        )
        column_names = [column[0] for column in results]
        tables_info.append(TableInfo(
            table_name=table_name,
            create_table_stmt=create_tbl_stmt,
            columns=column_names
        ))
    return tables_info


def _execute_query(connection, query):
    """Executes the query and returns the result."""
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


@contextmanager
def _setup_schematizer_container():
    """Set up a scheamtizer container and yields the IP address of the host
    container. It removes the container when exiting the context manager.
    """
    project = 'schematizermanualtest{}'.format(getpass.getuser())
    service = 'schematizerservice'
    try:
        _run_docker_compose_command(
            '--project-name={}'.format(project),
            'up',
            '-d',
            service
        )
        container_id = _ensure_containers_up(project, service)
        schematizer_host = _get_container_ip_address(container_id)
        _ensure_schematizer_is_ready(schematizer_host)
        yield schematizer_host

    finally:
        _run_docker_compose_command(
            '--project-name={}'.format(project),
            'kill'
        )
        _run_docker_compose_command(
            '--project-name={}'.format(project),
            'rm',
            '--force'
        )


def _ensure_containers_up(project, service):
    docker_client = Client(version='auto')
    service_container = docker_client.containers(
        filters={
            'label': ["com.docker.compose.project={}".format(project),
                      "com.docker.compose.service={}".format(service)]
        }
    )[0]
    container_id = service_container['Id']

    timeout_in_seconds = 5
    timed_out = time.time() + timeout_in_seconds
    while time.time() < timed_out:
        inspect_info = docker_client.inspect_container(container_id)
        container_status = inspect_info['State']['Status']
        if container_status == 'running':
            return container_id
        time.sleep(0.5)
    raise Exception("Unable to start up Schematizer container.")


def _get_container_ip_address(container_id):
    docker_client = Client(version='auto')
    inspect_info = docker_client.inspect_container(container_id)
    return inspect_info['NetworkSettings']['IPAddress']


def _ensure_schematizer_is_ready(schematizer_host):
    timeout_in_seconds = 60
    timed_out = time.time() + timeout_in_seconds
    while time.time() < timed_out:
        try:
            response = requests.get(
                url='http://{}:8888/status'.format(schematizer_host),
            )
            if response.status_code == 200:
                return
        except Exception:
            # Schematizer service isn't ready; wait and retry
            pass
        time.sleep(0.5)
    raise Exception("Schematizer container is not ready.")


def _run_docker_compose_command(*args):
    with open("logs/docker-compose.log", "a") as f:
        subprocess.call(
            ['docker-compose'] + list(args),
            stdout=f,
            stderr=subprocess.STDOUT
        )


def _register_tables(schematizer_host, tables_info):
    register_tables_results = []
    for table_info in tables_info:
        result = _register_table(schematizer_host, table_info)
        register_tables_results.append((table_info, result))
    return register_tables_results


def _register_table(schematizer_host, table_info):
    post_payload = _get_register_schema_payload(table_info)
    response = requests.post(
        url='http://{}:8888/v1/schemas/mysql'.format(schematizer_host),
        data=simplejson.dumps(post_payload),
        headers={
            'content-type': 'application/json',
            'Accept-Charset': 'UTF-8'
        }
    )
    if response.status_code == 200:
        return RegisterResult(
            success=True,
            result=simplejson.loads(response.json()['schema'])
        )
    return RegisterResult(success=False, result=response.text)


def _get_register_schema_payload(table_info):
    return {
        'namespace': 'schematizer_manual_test',
        'source': table_info.table_name,
        'source_owner_email': 'bam+schematizer_test@yelp.com',
        'new_create_table_stmt': table_info.create_table_stmt,
        'contains_pii': False
    }


def _verify_register_tables_results(register_tables_results):
    stats = _ResultStats()
    for result_tuple in register_tables_results:
        table_info, register_result = result_tuple
        stats.increment_result_count()
        if _is_successful_result(table_info, register_result):
            stats.record_success_result(table_info, register_result)
        else:
            stats.record_failed_result(table_info, register_result)
    return stats


def _is_successful_result(table_info, result):
    if not result.success:
        return False
    if table_info.table_name != result.result['name']:
        return False
    field_names = [field['name'] for field in result.result['fields']]
    if field_names != table_info.columns:
        return False
    return True


def _output_results(result_stats):
    print('Registered total {} tables.'.format(
        result_stats.total_count
    ))
    print('{} tables successfully registered.'.format(
        len(result_stats.success_results)
    ))
    print('{} table failed.'.format(
        len(result_stats.failed_results)
    ))
    for failed_result in result_stats.failed_results:
        table_info, register_result = failed_result
        print('{}: {}'.format(table_info, register_result))


class _ResultStats(object):

    def __init__(self):
        self.total_count = 0
        self.success_results = []
        self.failed_results = []

    def increment_result_count(self):
        self.total_count += 1

    def record_success_result(self, table_info, register_result):
        self.success_results.append((table_info, register_result))

    def record_failed_result(self, table_info, register_result):
        self.failed_results.append((table_info, register_result))


if __name__ == "__main__":
    parser = _setup_cli_options()
    run(parser.parse_args())
