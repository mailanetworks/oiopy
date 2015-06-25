from cliff import show
from cliff import command
from cliff import lister


class CreateContainer(command.Command):
    def get_parser(self, prog_name):
        parser = super(CreateContainer, self).get_parser(prog_name)
        parser.add_argument(
            'containers',
            metavar='<container-name>',
            nargs='+',
            help='New container name(s)'
        )
        return parser

    def take_action(self, parsed_args):
        for container in parsed_args.containers:
            self.app.storage.container_create(
                self.app.account_name,
                container
            )


class DeleteContainer(command.Command):
    def get_parser(self, prog_name):
        parser = super(DeleteContainer, self).get_parser(prog_name)
        parser.add_argument(
            'containers',
            metavar='<container>',
            nargs='+',
            help='Container(s) to delete'
        )
        return parser

    def take_action(self, parsed_args):
        for container in parsed_args.containers:
            self.app.storage.container_delete(
                self.app.account_name,
                container
            )


class ShowContainer(show.ShowOne):
    def get_parser(self, prog_name):
        parser = super(ShowContainer, self).get_parser(prog_name)
        parser.add_argument(
            'container',
            metavar='<container>',
            help='Container to show'
        )

        return parser

    def take_action(self, parsed_args):
        data = self.app.storage.container_show(
            self.app.account_name,
            parsed_args.container
        )
        return zip(*sorted(data.iteritems()))


class ListContainer(lister.Lister):
    def get_parser(self, prog_name):
        parser = super(ListContainer, self).get_parser(prog_name)

        parser.add_argument(
            '--prefix',
            metavar='<prefix>',
            help='Filter list using <prefix>'
        )
        parser.add_argument(
            '--marker',
            metavar='<marker>',
            help='Marker for paging'
        )
        parser.add_argument(
            '--end-marker',
            metavar='<end-marker>',
            help='End marker for paging'
        )
        parser.add_argument(
            '--delimiter',
            metavar='<delimiter>',
            help='Delimiter'
        )
        parser.add_argument(
            '--limit',
            metavar='<limit>',
            help='Limit of results to return'
        )
        return parser

    def take_action(self, parsed_args):

        columns = ('Name', 'Bytes', 'Count')

        kwargs = {}
        if parsed_args.prefix:
            kwargs['prefix'] = parsed_args.prefix
        if parsed_args.marker:
            kwargs['marker'] = parsed_args.marker
        if parsed_args.end_marker:
            kwargs['end_marker'] = parsed_args.end_marker
        if parsed_args.delimiter:
            kwargs['delimiter'] = parsed_args.delimiter
        if parsed_args.limit:
            kwargs['limit'] = parsed_args.limit

        l, meta = self.app.storage.container_list(
            self.app.account_name,
            **kwargs
        )

        results = ((v[0], v[2], v[1]) for v in l)
        return columns, results
