import argparse
from collections import namedtuple
import configparser
from pathlib import Path

import boto3
import psycopg2


Config = namedtuple('Config', ('aws_key', 'aws_secret', 'aws_region',
                               'cluster_type', 'num_nodes', 'node_type',
                               'cluster_identifier', 'db_name', 'db_user', 'db_password', 'port',
                               'iam_role_name', 'security_group'))


def parse_config_file(config_file) -> Config:
    """
    Parse configuration file.

    :param config_file: path to the configuration file
    :return: Config object containing all configurations
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
        config.get('DWH', 'IAM_ROLE_NAME'), config.get('DWH', 'SECURITY_GROUP')
    )


def create_cluster(session: boto3.Session, config: Config, args: argparse.Namespace) -> None:
    """
    Creates an AWS Redhisft cluster

    :param session: redshift client
    :param config: Config instance containing cluster configuration information
    :param args: unused (exists for API compatibility only)
    :return:
    """
    # Get iam role ARN
    iam = session.client('iam')
    role_arn = iam.get_role(RoleName=config.iam_role_name)['Role']['Arn']
    # Create cluster
    print('Creating cluster...')
    redshift = session.client('redshift')
    response = redshift.create_cluster(
        # Cluster
        ClusterType=config.cluster_type,
        NodeType=config.node_type,
        NumberOfNodes=config.num_nodes,
        # Identifiers & Credentials
        DBName=config.db_name,
        ClusterIdentifier=config.cluster_identifier,
        MasterUsername=config.db_user,
        MasterUserPassword=config.db_password,
        # Roles (for s3 access) and security group
        IamRoles=[role_arn],
        VpcSecurityGroupIds=['sg-02fec6f5813418cba']
        # ClusterSecurityGroups=[config.security_group]
    )
    # pprint(response) # TODO: write to logger at debug level
    # Wait for cluster to be available
    redshift.get_waiter('cluster_available').wait(ClusterIdentifier=config.cluster_identifier)
    # Display cluster properties
    cluster_props = redshift.describe_clusters(ClusterIdentifier=config.cluster_identifier)['Clusters'][0]
    endpoint = cluster_props['Endpoint']['Address']
    print(f'Cluster {config.cluster_identifier} successfully created and available.')
    print(f'    Cluster endpoint: {endpoint}')
    # # Create TCP connection to cluster
    # ec2 = session.resource('ec2')
    # vpc = ec2.Vpc(id=cluster_props['VpcId'])
    # default_security_group = list(vpc.security_groups.all())[0]
    # default_security_group.authorize_ingress(
    #     GroupName=default_security_group.group_name,
    #     CidrIp='0.0.0.0/0',
    #     IpProtocol='TCP',
    #     FromPort=config.port,
    #     ToPort=config.port
    # )
    # Test TCP connection
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
delete_cluster_parser.add_argument('--cluster-id', help='ID of an AWS Redshift cluster',
                                   default=None, dest='cluster_id')
delete_cluster_parser.set_defaults(func=delete_cluster, subparser=delete_cluster_parser)


if __name__ == '__main__':
    args = parser.parse_args()
    config = parse_config_file(args.config_file)
    session = boto3.Session(region_name=config.aws_region,
                            aws_access_key_id=config.aws_key,
                            aws_secret_access_key=config.aws_secret)
    args.func(session, config, args)
