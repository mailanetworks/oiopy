import logging

from cliff import command
from cliff import show
from oiopy.cli.utils import KeyValueAction


class ShowAccount(show.ShowOne):
    """Show account"""

    log = logging.getLogger(__name__ + '.ShowAccount')

    def get_parser(self, prog_name):
        parser = super(ShowAccount, self).get_parser(prog_name)
        parser.add_argument(
            'account',
            metavar='<account>',
            help='Account to update',
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        data = self.app.client_manager.storage.account_show(
            account=parsed_args.account
        )
        return zip(*sorted(data.iteritems()))


class CreateAccount(command.Command):
    """Create account"""

    log = logging.getLogger(__name__ + '.CreateAccount')

    def get_parser(self, prog_name):
        parser = super(CreateAccount, self).get_parser(prog_name)
        parser.add_argument(
            'accounts',
            metavar='<account>',
            nargs='+',
            help='Account(s) to create'
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        for account in parsed_args.accounts:
            data = self.app.client_manager.storage.account_create(
                account=account
            )
            self.log.debug('account create result: %s' % data)


class SetAccount(command.Command):
    """Set account"""

    log = logging.getLogger(__name__ + '.SetAccount')

    def get_parser(self, prog_name):
        parser = super(SetAccount, self).get_parser(prog_name)
        parser.add_argument(
            'account',
            metavar='<account>',
            help='Account to modify',
        )
        parser.add_argument(
            '-p',
            '--property',
            metavar='<key=value>',
            action=KeyValueAction,
            help='Property to add/update to this account'
        )
        parser.add_argument(
            '-d',
            '--delete-property',
            action='append',
            metavar='<key>',
            help='Property to delete for this account'
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        self.app.client_manager.storage.account_update(
            account=parsed_args.account,
            metadata=parsed_args.property,
            to_delete=parsed_args.delete_property
        )
