"""Command-line interface to the OpenIO APIs"""

import sys
import logging

import pkg_resources
from cliff import app
from cliff import commandmanager

import oiopy
from oiopy.object_storage import ObjectStorageAPI
from oiopy.directory import DirectoryAPI
from oiopy import utils
from oiopy.http import requests


class CommandManager(commandmanager.CommandManager):
    def __init__(self, namespace, convert_underscores=True):
        self.group_list = []
        super(CommandManager, self).__init__(namespace, convert_underscores)

        self.client_manager = None

    def load_commands(self, namespace):
        self.group_list.append(namespace)
        return super(CommandManager, self).load_commands(namespace)

    def add_command_group(self, group=None):
        if group:
            self.load_commands(group)

    def get_command_groups(self):
        return self.group_list

    def get_command_names(self, group=None):
        group_list = []
        if group is not None:
            for ep in pkg_resources.iter_entry_points(group):
                cmd_name = (
                    ep.name.replace('_', ' ')
                    if self.convert_underscores
                    else ep.name
                )
                group_list.append(cmd_name)
            return group_list
        return self.commands.keys()


class OpenIOShell(app.App):
    log = logging.getLogger(__name__)

    def __init__(self):
        super(OpenIOShell, self).__init__(
            description=__doc__.strip(),
            version=oiopy.__version__,
            command_manager=CommandManager('oiopy.cli'),
            deferred_help=True)
        self.storage = None
        self.directory = None
        self.requests_session = None

    def configure_logging(self):
        super(OpenIOShell, self).configure_logging()
        root_logger = logging.getLogger('')

        if self.options.verbose_level == 0:
            root_logger.setLevel(logging.ERROR)
        elif self.options.verbose_level == 1:
            root_logger.setLevel(logging.WARNING)
        elif self.options.verbose_level == 2:
            root_logger.setLevel(logging.INFO)
        elif self.options.verbose_level >= 3:
            root_logger.setLevel(logging.DEBUG)


        requests_log = logging.getLogger('requests')

        if self.options.debug:
            requests_log.setLevel(logging.DEBUG)
        else:
            requests_log.setLevel(logging.ERROR)


        cliff_log = logging.getLogger('cliff')
        cliff_log.setLevel(logging.ERROR)

        stevedore_log = logging.getLogger('stevedore')
        stevedore_log.setLevel(logging.ERROR)

    def run(self, argv):
        try:
            return super(OpenIOShell, self).run(argv)
        except Exception as e:
            self.log.error('Exception raised: ' + str(e))
            return 1

    def build_option_parser(self, description, version):
        parser = super(OpenIOShell, self).build_option_parser(
            description,
            version)

        parser.add_argument(
            '--oio-ns',
            metavar='<namespace>',
            dest='ns',
            default=utils.env('OIO_NS'),
            help='Namespace name (Env: OIO_NS)',
        )
        parser.add_argument(
            '--oio-account',
            metavar='<account>',
            dest='account_name',
            default=utils.env('OIO_ACCOUNT'),
            help='Account name (Env: OIO_ACCOUNT)'
        )
        parser.add_argument(
            '--oio-proxyd-url',
            metavar='<proxyd url>',
            dest='proxyd_url',
            default=utils.env('OIO_PROXYD_URL'),
            help='Proxyd URL (Env: OIO_PROXYD_URL)'
        )

        return parser

    def initialize_app(self, argv):
        super(OpenIOShell, self).initialize_app(argv)

        self.command_manager.add_command_group('openio.storage')
        self.command_manager.add_command_group('openio.directory')

        self.ns = self.options.ns
        self.account_name = self.options.account_name
        self.proxyd_url = self.options.proxyd_url

        self.print_help_if_requested()

        self.requests_session = requests.Session()
        if not self.ns:
            raise RuntimeError('namespace not specified')
        if not self.proxyd_url:
            raise RuntimeError('Proxyd URL not specified')
        self.storage = ObjectStorageAPI(self.ns, self.proxyd_url,
                                        session=self.requests_session)
        self.directory = DirectoryAPI(self.ns, self.proxyd_url,
                                      session=self.requests_session)

    def prepare_to_run_command(self, cmd):
        self.log.info(
            'command: %s -> %s.%s',
            getattr(cmd, 'cmd_name', '<none>'),
            cmd.__class__.__module__,
            cmd.__class__.__name__,
        )

    def clean_up(self, cmd, result, err):
        self.log.debug('clean up %s: %s', cmd.__class__.__name__, err or '')



def main(argv=sys.argv[1:]):
    return OpenIOShell().run(argv)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
