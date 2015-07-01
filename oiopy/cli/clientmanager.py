import logging
import sys
import pkg_resources
from oiopy.http import requests
from oiopy import exceptions


LOG = logging.getLogger(__name__)
PLUGIN_MODULES = []


def validate_options(options):
    msg = ''
    if not options.get('namespace', None):
        msg += 'Set a namespace with --oio-ns, OIO_NS\n'
    if not options.get('proxyd_url', None):
        msg += 'Set a proxyd URL with --oio-proxyd-url, OIO_PROXYD_URL\n'
    if msg:
        raise exceptions.CommandError('Missing parameter(s): \n%s' % msg)


class ClientCache(object):
    def __init__(self, factory):
        self.factory = factory
        self._handle = None

    def __get__(self, instance, owner):
        if self._handle is None:
            self._handle = self.factory(instance)
        return self._handle


class ClientManager(object):
    def __init__(self, options):
        self._options = options
        self.session = None
        self.namespace = None
        self.setup_done = False
        root_logger = logging.getLogger('')
        LOG.setLevel(root_logger.getEffectiveLevel())

    def setup(self):
        if not self.setup_done:
            validate_options(self._options)
            self.namespace = self._options['namespace']
            LOG.info('Using parameters %s' % self._options)
            self.session = requests.Session()
            self.setup_done = True

    def get_endpoint(self, service_type):
        self.setup()
        # TODO: for the moment always return the proxyd URL
        endpoint = self._options['proxyd_url']
        return endpoint

    def get_account(self):
        account_name = self._options.get('account_name', None)
        if not account_name:
            msg = 'Set an account name with --oio-account, OIO_ACCOUNT\n'
            raise exceptions.CommandError('Missing parameter: \n%s' % msg)
        return account_name


def get_plugin_modules(group):
    modules_list = []
    for entry_point in pkg_resources.iter_entry_points(group):
        LOG.debug('Found plugin %r', entry_point.name)

        __import__(entry_point.module_name)
        module = sys.modules[entry_point.module_name]
        modules_list.append(module)

        client_cache = ClientCache(
            getattr(module, 'make_client', None)
        )

        setattr(
            ClientManager,
            module.API_NAME,
            client_cache
        )
    return modules_list


def build_plugin_option_parser(parser):
    for module in PLUGIN_MODULES:
        parser = module.build_option_parser(parser)
    return parser

PLUGIN_MODULES = get_plugin_modules(
    'openio.cli.base'
)

PLUGIN_MODULES.extend(get_plugin_modules(
    'openio.cli.ext'
))
