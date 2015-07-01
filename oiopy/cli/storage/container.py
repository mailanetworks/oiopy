import logging
from cliff import show
from cliff import command
from cliff import lister

from oiopy.cli.utils import KeyValueAction


class CreateContainer(command.Command):
    """Create container"""

    log = logging.getLogger(__name__ + '.CreateContainer')

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
        self.log.debug('take_action(%s)', parsed_args)

        for container in parsed_args.containers:
            self.app.client_manager.storage.container_create(
                self.app.client_manager.get_account(),
                container
            )


class SetContainer(command.Command):
    """Set container"""

    log = logging.getLogger(__name__ + '.SetContainer')

    def get_parser(self, prog_name):
        parser = super(SetContainer, self).get_parser(prog_name)
        parser.add_argument(
            'container',
            metavar='<container>',
            help='Container name'
        )
        parser.add_argument(
            '--property',
            metavar='<key=value>',
            action=KeyValueAction,
            help='Set a property on <container>'
        )
        parser.add_argument(
            '--clear',
            dest='clear',
            default=False,
            help='Clear previous properties',
            action="store_true"
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        metadata = {}
        for m in parsed_args.metadata:
            k, v = m.split(':', 1)
            metadata['user.%s' % k] = v
        self.app.client_manager.storage.container_update(
            self.app.client_manager.get_account(),
            parsed_args.container,
            metadata,
            clear=parsed_args.clear
        )


class DeleteContainer(command.Command):
    """Delete container"""

    log = logging.getLogger(__name__ + '.DeleteContainer')

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
        self.log.debug('take_action(%s)', parsed_args)

        for container in parsed_args.containers:
            self.app.client_manager.storage.container_delete(
                self.app.client_manager.get_account(),
                container
            )


class ShowContainer(show.ShowOne):
    """Show container"""

    log = logging.getLogger(__name__ + '.ShowContainer')

    def get_parser(self, prog_name):
        parser = super(ShowContainer, self).get_parser(prog_name)
        parser.add_argument(
            'container',
            metavar='<container>',
            help='Container to show'
        )

        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        data = self.app.client_manager.storage.container_show(
            self.app.client_manager.get_account(),
            parsed_args.container
        )
        return zip(*sorted(data.iteritems()))


class ListContainer(lister.Lister):
    """List container"""

    log = logging.getLogger(__name__ + '.ListContainer')

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
        self.log.debug('take_action(%s)', parsed_args)

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

        l, meta = self.app.client_manager.storage.container_list(
            self.app.client_manager.get_account(),
            **kwargs
        )

        columns = ('Name', 'Bytes', 'Count')

        results = ((v[0], v[2], v[1]) for v in l)
        return columns, results
