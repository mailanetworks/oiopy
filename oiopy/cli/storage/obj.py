import io
import os

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
        account = self.app.account_name
        container = parsed_args.container

        def get_file_size(f):
            currpos = f.tell()
            f.seek(0, 2)
            total_size = f.tell()
            f.seek(currpos)
            return total_size

        for obj in parsed_args.objects:
            with io.open(obj, 'rb') as f:
                self.app.storage.object_create(
                    account,
                    container,
                    file_or_path=f,
                    content_length=get_file_size(f)
                )

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
        account = self.app.account_name
        container = parsed_args.container

        for obj in parsed_args.objects:
            self.app.storage.object_delete(
                account,
                container,
                obj
            )

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
        account = self.app.account_name
        container = parsed_args.container

        data = self.app.storage.object_show(
            account,
            container,
            parsed_args.object
        )
        return zip(*sorted(data.iteritems()))

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
        parser.add_argument(
            '--size',
            metavar='<size>',
            type=int,
            help='Number of bytes to fetch'
        )
        parser.add_argument(
            '--offset',
            metavar='<offset>',
            type=int,
            help='Fetch data from <offset>'
        )
        return parser

    def take_action(self, parsed_args):
        account = self.app.account_name
        container = parsed_args.container
        obj = parsed_args.object

        file = parsed_args.file        
        if not file:
            file = obj
        size = parsed_args.size
        offset = parsed_args.offset

        meta, stream = self.app.storage.object_fetch(
            account,
            container,
            obj,
            size=size,
            offset=offset
        )
        if not os.path.exists(os.path.dirname(file)):
            if len(os.path.dirname(file)) > 0:
                os.makedirs(os.path.dirname(file))
        with open(file, 'wb') as f:
            for chunk in stream:
                f.write(chunk)

class ListObject(lister.Lister):
    """List objects in container"""

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
        return parser

    def take_action(self, parsed_args):
        account = self.app.account_name
        container = parsed_args.container

        resp = self.app.storage.object_list(
            account,
            container,
            limit=parsed_args.limit,
            marker=parsed_args.marker,
            end_marker=parsed_args.end_marker,
            prefix=parsed_args.prefix,
            delimiter=parsed_args.delimiter
        )
        l = resp['objects']
        results = ((obj['name'], obj['size'], obj['hash']) for obj in l)
        columns = ('Name', 'Size', 'Hash')
        return (columns, results)

