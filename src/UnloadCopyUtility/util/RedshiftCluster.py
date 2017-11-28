import datetime
import logging
import re

import boto3
import pg

options = """keepalives=1 keepalives_idle=200 keepalives_interval=200
             keepalives_count=6"""

set_timeout_stmt = "set statement_timeout = 1200000"


class RedshiftCluster:
    def __init__(self, cluster_endpoint):
        self._password = None
        self._user = None
        self._db = None
        self._host = None
        self._port = None
        self.cluster_endpoint = cluster_endpoint
        self._user_auto_create = False
        self._user_creds_expiration = datetime.datetime.now()
        self._user_db_groups = []

    def get_user(self):
        return self._user

    def set_user(self, user):
        self._user = user

    def get_password(self):
        if self._password is None or self.is_temporary_credential_expired():
            self.refresh_temporary_credentials()
        return self._password

    def set_password(self, password):
        self._password = password

    def get_host(self):
        return self._host

    def set_host(self, host):
        self._host = host


    def get_port(self):
        return self._port

    def set_port(self, port):
        self._port = port

    def get_db(self):
        return self._db

    def set_db(self, db):
        self._db = db

    def get_user_auto_create(self):
        return self._user_auto_create

    def set_user_auto_create(self, user_auto_create):
        self._user_auto_create = user_auto_create


    def get_user_db_groups(self):
        return self._user_db_groups

    def set_user_db_groups(self, user_db_groups):
        self._user_db_groups = user_db_groups

    def get_user_creds_expiration(self):
        return self._user_creds_expiration

    def set_user_creds_expiration(self, user_creds_expiration):
        self._user_creds_expiration = user_creds_expiration


    def is_temporary_credential_expired(self):
        one_minute_from_now = datetime.datetime.now() + datetime.timedelta(minutes=1)
        if self.get_user_creds_expiration() is None:
            return True

        expiration_time = self.get_user_creds_expiration()
        if one_minute_from_now > expiration_time:
            return True
        return False

    def refresh_temporary_credentials(self):
        logging.debug("Try getting DB credentials for {u}@{c}".format(u=self.get_user(), c=self.get_host()))
        redshift_client = boto3.client('redshift', region_name=self.get_region_name())
        get_creds_params = {
            'DbUser': self.get_user(),
            'DbName': self.get_db(),
            'ClusterIdentifier': self.get_cluster_identifier()
        }
        if self.get_user_auto_create():
            get_creds_params['AutoCreate'] = True
        if len(self.get_user_db_groups()) > 0:
            get_creds_params['DbGroups'] = self.get_user_db_groups()
        response = redshift_client.get_cluster_credentials(**get_creds_params)
        self.set_user(response['DbUser'])
        self.set_password(response['DbPassword'])
        self.set_user_creds_expiration(response['Expiration'])

    @staticmethod
    def get_cluster_endpoint_regex():
        """
        A cluster endpoint is comprised of letters, digits, or hyphens

        From http://docs.aws.amazon.com/redshift/latest/mgmt/managing-clusters-console.html
            They must contain from 1 to 63 alphanumeric characters or hyphens.
            Alphabetic characters must be lowercase.
            The first character must be a letter.
            They cannot end with a hyphen or contain two consecutive hyphens.
            They must be unique for all clusters within an AWS account.
        :return:
        """
        cluster_endpoint_regex_parts = [
            {
                'name': 'cluster_identifier',
                'pattern': '[a-z][a-z0-9-]*'
            },
            {
                'pattern': r'\.'
            },
            {
                'name': 'customer_hash',
                'pattern': r'[0-9a-z]+'
            },
            {
                'pattern': r'\.'
            },
            {
                'name': 'region',
                'pattern': '[a-z]+-[a-z]+-[0-9]+'
            },
            {
                'pattern': r'\.redshift\.amazonaws\.com$'
            }
        ]
        pattern = ''
        for element in cluster_endpoint_regex_parts:
            if 'name' in element.keys():
                pattern += '(?P<' + element['name'] + '>'
            pattern += element['pattern']
            if 'name' in element.keys():
                pattern += ')'
        return re.compile(pattern)

    def get_element_from_cluster_endpoint(self, element):
        match_result = RedshiftCluster.get_cluster_endpoint_regex().match(self.cluster_endpoint.lower())
        if match_result is not None:
            return match_result.groupdict()[element]
        else:
            logging.fatal('Could not extract region from cluster endpoint {cluster_endpoint}'.format(
                cluster_endpoint=self.cluster_endpoint.lower()))

    def get_region_name(self):
        return self.get_element_from_cluster_endpoint('region')

    def get_cluster_identifier(self):
        return self.get_element_from_cluster_endpoint('cluster_identifier')

    def _conn_to_rs(self, opt=options, timeout=set_timeout_stmt):
        rs_conn_string = "host={host} port={port} dbname={db} user={user} password={password} {opt}".format(
            host=self.get_host(),
            port=self.get_port(),
            db=self.get_db(),
            password=self.get_password(), # Very important to first fetch the password because temporary password updates user!
            user=self.get_user(),
            opt=opt)
        print("Connecting to {host}:{port}:{db} as {user}".format(host=self.get_host(),
                                                                  port=self.get_port(),
                                                                  db=self.get_db(),
                                                                  user=self.get_user()))
        rs_conn = pg.connect(dbname=rs_conn_string)
        self._conn = rs_conn

    def execute_query(self, command, opt=options, timeout=set_timeout_stmt):
        self._conn_to_rs(opt=options, timeout=set_timeout_stmt)
        self._conn.query(timeout)
        print(command)
        self._conn.query(command)
        self._disconnect_from_rs()

    def _disconnect_from_rs(self):
        self._conn.close()