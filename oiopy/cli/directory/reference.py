import logging

from cliff import command
from cliff import lister


class ShowReference(lister.Lister):
    """Show reference"""

    log = logging.getLogger(__name__ + '.ShowReference')

    def get_parser(self, prog_name):
        parser = super(ShowReference, self).get_parser(prog_name)
        parser.add_argument(
            'reference',
            metavar='<reference>',
            help='Reference to show'
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        data = self.app.client_manager.directory.get(
            self.app.client_manager.get_account(),
            reference=parsed_args.reference
        )
        columns = ('Type', 'Host', 'Args', 'Seq')
        results = ((d['type'], d['host'], d['args'], d['seq'])
                   for d in data['srv'])
        return columns, results


class CreateReference(command.Command):
    """Create reference"""

    log = logging.getLogger(__name__ + '.CreateReference')

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
        self.log.debug('take_action(%s)', parsed_args)

        for reference in parsed_args.references:
            self.app.client_manager.directory.create(
                self.app.client_manager.get_account(),
                reference=reference
            )


class DeleteReference(command.Command):
    """Delete reference"""

    log = logging.getLogger(__name__ + '.DeleteReference')

    def get_parser(self, prog_name):
        parser = super(DeleteReference, self).get_parser(prog_name)
        parser.add_argument(
            'references',
            metavar='<reference>',
            nargs='+',
            help='Reference(s) to delete'
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        for reference in parsed_args.references:
            self.app.client_manager.directory.delete(
                self.app.client_manager.get_account(),
                reference=reference
            )


class LinkReference(command.Command):
    """Link services to reference"""

    log = logging.getLogger(__name__ + '.LinkReference')

    def get_parser(self, prog_name):
        parser = super(LinkReference, self).get_parser(prog_name)
        parser.add_argument(
            'reference',
            metavar='<reference>',
            help='Reference to update'
        )
        parser.add_argument(
            'srv_type',
            metavar='<srv_type>',
            help='Link services of <srv_type> to reference'
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        reference = parsed_args.reference
        srv_type = parsed_args.srv_type

        self.app.client_manager.directory.link(
            self.app.client_manager.get_account(),
            reference,
            srv_type
        )


class UnlinkReference(command.Command):
    """Unlink services from reference"""

    log = logging.getLogger(__name__ + '.UnlinkReference')

    def get_parser(self, prog_name):
        parser = super(UnlinkReference, self).get_parser(prog_name)
        parser.add_argument(
            'reference',
            metavar='<reference>',
            help='Reference to unlink'
        )
        parser.add_argument(
            'srv_type',
            metavar='<srv_type>',
            help='Unlink services of <srv_type> from reference'
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        reference = parsed_args.reference
        srv_type = parsed_args.srv_type

        self.app.client_manager.directory.unlink(
            self.app.client_manager.get_account(),
            reference,
            srv_type
        )


class PollReference(command.Command):
    """Poll services for reference"""

    log = logging.getLogger(__name__ + '.PollReference')

    def get_parser(self, prog_name):
        parser = super(PollReference, self).get_parser(prog_name)
        parser.add_argument(
            'reference',
            metavar='<reference>',
            help='Reference to poll'
        )
        parser.add_argument(
            'srv_type',
            metavar='<srv_type>',
            help='Poll services of <srv_type>'
        )
        return parser

    def take_action(self, parsed_args):
        reference = parsed_args.reference
        srv_type = parsed_args.srv_type

        self.app.client_manager.directory.renew(
            self.app.client_manager.get_account(),
            reference,
            srv_type
        )


class ForceReference(command.Command):
    """Force link a service to reference"""

    log = logging.getLogger(__name__ + '.ForceReference')

    def get_parser(self, prog_name):
        parser = super(ForceReference, self).get_parser(prog_name)
        parser.add_argument(
            'reference',
            metavar='<reference>',
            help='Reference to update'
        )
        parser.add_argument(
            'service',
            metavar='<service>',
            help='Service to force link to reference'
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)

        reference = parsed_args.reference
        service = parsed_args.service

        self.app.client_manager.directory.force(
            self.app.client_manager.get_account(),
            reference,
            service
        )


class SetReference(command.Command):
    """Set reference properties"""

    log = logging.getLogger(__name__ + '.SetReference')

    def get_parser(self, prog_name):
        parser = super(SetReference, self).get_parser(prog_name)
        parser.add_argument(
            'reference',
            metavar='<reference>',
            help='Reference to set'
        )
        return parser

    def take_action(self, parsed_args):
        self.log.debug('take_action(%s)', parsed_args)
