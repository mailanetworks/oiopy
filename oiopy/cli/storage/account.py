from cliff import show
from cliff import command
from oiopy.cli.utils import KeyValueAction



class ShowAccount(show.ShowOne):
    """Show account"""

    def get_parser(self, prog_name):
        parser = super(ShowAccount, self).get_parser(prog_name)
        parser.add_argument(
            'account',
            metavar='<account>',
            help='Account to update',
        )
        return parser

    def take_action(self, parsed_args):
        data = self.app.storage.account_show(
            account=parsed_args.account
        )
        return zip(*sorted(data.iteritems()))

class CreateAccount(command.Command):
    """Create account"""

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
        for account in parsed.args.accounts:
            data = self.app.storage.account_create(
                account=account
            )

class SetAccount(command.Command):
    """Set account"""

    def get_parser(self, prog_name):
        parser = super(UpdateAccount, self).get_parser(prog_name)
        parser.add_argument(
            'account',
            metavar='<account>',
            help='Account to modify',
        )
        parser.add_argument(
            '--property',
            metavar='<key=value>',
            action=KeyValueAction,
            help='Property to add to this account'
        )
        return parser

    def take_action(self, parsed_args):
        metadata = {}
        for m in parsed_args.metadatas:
            k, v = m.split(':', 1)
            metadata[k] = v
        self.app.storage.account_update(
            account=parsed_args.account,
            metadata=metadata
        )

