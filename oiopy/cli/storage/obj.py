import io
import logging
import os

from cliff import command
from cliff import lister
from cliff import show

from oiopy.cli.utils import KeyValueAction


class CreateObject(command.Command):
    """Upload object"""

    log = logging.getLogger(__name__ + '.CreateObject')

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
        self.log.debug('take_action(%s)', parsed_args)

        container = parsed_args.container

        def get_file_size(f):
            currpos = f.tell()
            f.seek(0, 2)
            total_size = f.tell()
            f.seek(currpos)
            return total_size

        for obj in parsed_args.objects:
            with io.open(obj, 'rb') as f:
                self.app.client_manager.storage.object_create(
                    self.app.client_manager.get_account(),
                    container,
                    file_or_path=f,
                    content_length=get_file_size(f)
                )


class DeleteObject(command.Command):
    """Delete object from container"""

    log = logging.getLogger(__name__ + '.DeleteObject')

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
        self.log.debug('take_action(%s)', parsed_args)

        container = parsed_args.container

        for obj in parsed_args.objects:
            self.app.client_manager.storage.object_delete(
                self.app.client_manager.get_account(),
                container,
                obj
            )


class ShowObject(show.ShowOne):
    """Show object"""

    log = logging.getLogger(__name__ + '.ShowObject')

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
        self.log.debug('take_action(%s)', parsed_args)

        container = parsed_args.container

        data = self.app.client_manager.storage.object_show(
            self.app.client_manager.get_account(),
            container,
            parsed_args.object
        )
        return zip(*sorted(data.iteritems()))


class SetObject(command.Command):
    """Set object"""

    log = logging.getLogger(__name__ + '.SetObject')

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
        self.log.debug('take_action(%s)', parsed_args)


class SaveObject(command.Command):
    """Save object locally"""

    log = logging.getLogger(__name__ + '.SaveObject')

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
        self.log.debug('take_action(%s)', parsed_args)

        container = parsed_args.container
        obj = parsed_args.object

        file = parsed_args.file
        if not file:
            file = obj
        size = parsed_args.size
        offset = parsed_args.offset

        meta, stream = self.app.client_manager.storage.object_fetch(
            self.app.client_manager.get_account(),
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

    log = logging.getLogger(__name__ + '.ListObject')

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
        self.log.debug('take_action(%s)', parsed_args)

        container = parsed_args.container

        resp = self.app.client_manager.storage.object_list(
            self.app.client_manager.get_account(),
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
