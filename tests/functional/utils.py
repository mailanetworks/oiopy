from testtools import TestCase
import json
import os


class FunctionalTestCase(TestCase):
    def __init__(self, *args, **kwargs):
        super(FunctionalTestCase, self).__init__(*args, **kwargs)
        self.load_config()

    def load_config(self):
        default_conf_path = os.path.expanduser('~/.oio/sds/conf/test.conf')
        config_file = os.environ.get('SDS_TEST_CONFIG_FILE',
                                     default_conf_path)
        with open(config_file, 'r') as f:
            self.conf = json.load(f)
        self.proxyd_uri = self.conf['proxyd_uri']
        self.namespace = self.conf['namespace']
        self.account = self.conf['account']
        self.chunk_size = self.conf['chunk_size']
