from cliff import show
from cliff import command


class ShowReference(show.ShowOne):
    def get_parser(self, prog_name):
        parser = super(ShowReference, self).get_parser(prog_name)
        parser.add_argument(
            'reference',
            metavar='<reference>',
            help='Reference to show'
        )
        return parser

    def take_action(self, parsed_args):
        data = self.app.directory.get(
            account=self.app.account_name,
            reference=parsed_args.reference
        )
        return zip(*sorted(data.iteritems()))


class CreateReference(command.Command):
    def get_parser(self, prog_name):
        parser = super(CreateReference, self).get_parser(prog_name)
        parser.add_argument(
            'references',
            metavar='<reference>',
            nargs='+',
            help='Reference(s) to create'
        )
        return parser

    def take_action(self, parsed_args):
        for reference in parsed_args.references:
            self.app.directory.create(
                account=self.app.account_name,
                reference=reference
            )


class UpdateReference(command.Command):
    def get_parser(self, prog_name):
        parser = super(UpdateReference, self).get_parser(prog_name)
        parser.add_argument(
            'reference',
            metavar='<reference>',
            help='Reference to update'
        )
        return parser

    def take_action(self, parsed_args):
        pass