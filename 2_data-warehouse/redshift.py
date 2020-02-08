import argparse
from collections import namedtuple
import configparser
from pathlib import Path

import boto3
import psycopg2


Config = namedtuple('Config', ('aws_key', 'aws_secret', 'aws_region',
                               'cluster_type', 'num_nodes', 'node_type',
                               'cluster_identifier', 'db_name', 'db_user', 'db_password', 'port',
                               'iam_role_name', 'security_group_id'))


def parse_config_file(config_file: str) -> Config:
    """
    Parse configuration file for creating/deleting redshift clusters.

    :param config_file: path to the configuration file
    :return config: Config object containing all configurations
    """
    config = configparser.ConfigParser()
    with open(config_file) as f:
        config.read_file(f)
    return Config(
        # AWS authentication data
        config.get('AWS', 'KEY'), config.get('AWS', 'SECRET'), config.get('AWS', 'REGION'),
        # Redshift cluster hardware configuration
        config.get('DWH', 'CLUSTER_TYPE'), int(config.get('DWH', 'NUM_NODES')), config.get('DWH', 'NODE_TYPE'),
        # Redshift cluster authentication
        config.get('DWH', 'CLUSTER_IDENTIFIER'), config.get('DWH', 'DB_NAME'), config.get('DWH', 'DB_USER'),
        config.get('DWH', 'DB_PASSWORD'), int(config.get('DWH', 'PORT')),
        # IAM role (for S3 access) and security group
        config.get('DWH', 'IAM_ROLE_NAME'), config.get('DWH', 'SECURITY_GROUP_ID')
    )


def create_cluster(session: boto3.Session, config: Config, args: argparse.Namespace) -> None:
    """
    Creates an AWS Redhisft cluster

    :param session: a boto3.Session instance
    :param config: Config instance containing cluster configuration information
    :param args: unused (exists for API compatibility only)
    """
    role_arn = _get_iam_role_arn(config.iam_role_name, session)
    endpoint = _create_cluster(config, role_arn, session)
    _test_database_tcp_connection(config, endpoint)


def _get_iam_role_arn(iam_role_name: str, session: boto3.Session) -> str:
    """
    Returns the IAM Role Arn of the given IAM Role Name.

    :param iam_role_name: a IAM Role Name string
    :param session: a boto3.Session instance
    :return role_arn: the corresponding IAM Role ARN string
    """
    iam = session.client('iam')
    role_arn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']
    return role_arn


def _create_cluster(config: Config, role_arn: str, session: boto3.Session) -> str:
    """
    Creates a redshift cluster and returns its endpoint.

    :param config: a Config instnce
    :param role_arn: a IAM Role ARN string
    :param session: a boto3.Session instance
    :return endpoint: the endpoint for the redshift cluster
    """
    print('Creating cluster...')
    redshift = session.client('redshift')
    response = redshift.create_cluster(
        # Cluster
        ClusterType=config.cluster_type, NodeType=config.node_type, NumberOfNodes=config.num_nodes,
        # Identifiers & Credentials
        DBName=config.db_name, ClusterIdentifier=config.cluster_identifier,
        MasterUsername=config.db_user, MasterUserPassword=config.db_password,
        # Roles (for s3 access) and security group
        IamRoles=[role_arn], VpcSecurityGroupIds=[config.security_group_id]
    )
    # pprint(response) # TODO: write to logger at debug level
    # Wait for cluster to be available
    redshift.get_waiter('cluster_available').wait(ClusterIdentifier=config.cluster_identifier)
    # Display cluster properties
    cluster_props = redshift.describe_clusters(ClusterIdentifier=config.cluster_identifier)['Clusters'][0]
    endpoint = cluster_props['Endpoint']['Address']
    print(f'Cluster {config.cluster_identifier} successfully created and available.')
    print(f'    Cluster endpoint: {endpoint}')
    return endpoint


def _test_database_tcp_connection(config: Config, endpoint: str) -> None:
    """
    Tests that the TCP connection to a redshift cluster works properly.

    :param config: a Config instance
    :param endpoint: the endpoint to the redshift cluster
    """
    try:
        conn = psycopg2.connect(database=config.db_name, user=config.db_user, password=config.db_password,
                                host=endpoint, port=config.port)
    except Exception as e:
        print('Could not connect to cluster.')
        print(e)
    else:
        print('TCP connection to cluster successfully created.')
        conn.close()


def delete_cluster(session: boto3.Session, config: Config, args: argparse.Namespace) -> None:
    """
    Deletes a redshift cluster.

    This function tries to take args.cluster_id as the cluster identifier for deletion. If the command is run without
    the --cluster-id option, then it deletes the cluster identified as `config.cluster_identifier`

    :param session: a boto3.Session instance
    :param config: Config instance containing cluster configuration information
    :param args: argparse.Namespace object containing a `cluster_id` field; this parameter
    """
    cluster_id = config.cluster_identifier if args.cluster_id is None else args.cluster_id
    print(f'Deleting cluster {cluster_id}...')
    redshift = session.client('redshift')
    response = redshift.delete_cluster(ClusterIdentifier=cluster_id,  SkipFinalClusterSnapshot=True)
    # pprint(response) # TODO: write to logger at debug level
    # Wait for cluster to be deleted
    redshift.get_waiter('cluster_deleted').wait(ClusterIdentifier=cluster_id)
    print('Cluster successfully deleted.')


parser = argparse.ArgumentParser(description='Manage AWS Redshift clusters')
redshift_action_parsers = parser.add_subparsers(help='AWS Redshift action to be performed',
                                                title='sub-commands')
parser.add_argument('--config-file', help='The location of the config file',
                    default=Path(__file__, '..', 'admin.cfg').resolve().as_posix(),
                    type=str, dest='config_file')
# Add a "create" sub-command to create clusters
create_cluster_parser = redshift_action_parsers.add_parser('create', help='Create an AWS Redshift cluster')
create_cluster_parser.set_defaults(func=create_cluster, subparser=create_cluster_parser)
# Add a "delete" sub-command to delete clusters
delete_cluster_parser = redshift_action_parsers.add_parser('delete', help='Delete an AWS Redshift cluster')
delete_cluster_parser.add_argument('--cluster-id', help='ID of an AWS Redshift cluster (defaults to '
                                                        'CLUSTER_IDENTIFIER field from config file',
                                   default=None, dest='cluster_id')
delete_cluster_parser.set_defaults(func=delete_cluster, subparser=delete_cluster_parser)


if __name__ == '__main__':
    args = parser.parse_args()
    config = parse_config_file(args.config_file)
    session = boto3.Session(region_name=config.aws_region,
                            aws_access_key_id=config.aws_key,
                            aws_secret_access_key=config.aws_secret)
    args.func(session, config, args)
