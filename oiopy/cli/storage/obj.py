from cliff import command
from cliff import show
from cliff import lister

class CreateObject(command.Command):
    """Upload object"""

    def get_parser(self, prog_name):
        parser = super(CreateObject, self).get_parser(prog_name)
        parser.add_argument(
            'container',
            metavar='<container>',
            help='Container for new object'
        )
        parser.add_argument(
            'objects',
            metavar='<filename>',
            nargs='+',
            help='Local filename(s) to upload'
        )
        return parser

    def take_action(self, parsed_args):
        pass

class DeleteObject(command.Command):
    """Delete object from container"""

    def get_parser(self, prog_name):
        parser = super(DeleteObject, self).get_parser(prog_name)
        parser.add_argument(
            'container',
            metavar='<container>',
            help='Delete object(s) from <container>'
        )
        parser.add_argument(
            'objects',
            metavar='<object>',
            nargs='+',
            help='Object(s) to delete'
        )
        return parser

    def take_action(self, parsed_args):
        pass

class ShowObject(show.ShowOne):
    """Show object"""

    def get_parser(self, prog_name):
        parser = super(ShowObject, self).get_parser(prog_name)
        parser.add_argument(
            'container',
            metavar='<container>',
            help='Container'
        )
        parser.add_argument(
            'object',
            metavar='<object>',
            help='Object'
        )
        return parser

    def take_action(self, parsed_args):
        pass

class SetObject(command.Command):
    """Set object"""

    def get_parser(self, prog_name):
        parser = super(SetObject, self).get_parser(prog_name)
        parser.add_argument(
            'container',
            metavar='<container>',
            help='Container'
        )
        parser.add_argument(
            '--property',
            metavar='<key=value>',
            action=KeyValueAction,
            help='Property to add to this object'
        )
        return parser

    def take_action(self, parsed_args):
        pass

class SaveObject(command.Command):
    """Save object locally"""

    def get_parser(self, prog_name):
        parser = super(SaveObject, self).get_parser(prog_name)
        parser.add_argument(
            '--file',
            metavar='<filename>',
            help='Destination filename (defaults to object name)'
        )
        parser.add_argument(
            'container',
            metavar='<container>',
            help='Download <object> from <container>'
        )
        parser.add_argument(
            'object',
            metavar='<object>',
            help='Object to save'
        )
        return parser

    def take_action(self, parsed_args):
        pass

class ListObject(lister.Lister):
    """List objects"""

    def get_parser(self, prog_name):
        parser = super(ListObject, self).get_parser(prog_name)
        parser.add_argument(
            'container',
            metavar='<container>',
            help='Container to list'
        )
        parser.add_argument(
            '--prefix',
            metavar='<prefix>',
            help='Filter list using <prefix>'
        )
        parser.add_argument(
            '--delimiter',
            metavar='<delimiter>',
            help='Filter list using <delimiter>'
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
            '--limit',
            metavar='<limit>',
            help='Limit the number of objects returned'
        )

    def take_action(self, parsed_args):
        pass
