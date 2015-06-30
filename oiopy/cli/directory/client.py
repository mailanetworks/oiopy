import logging
from oiopy import utils

LOG = logging.getLogger(__name__)

API_NAME = 'directory'

def make_client(instance):
    endpoint = instance.get_endpoint('directory')
    client = DirectoryAPI(
        session=session.instance,
        endpoint=endpoint
    )
    return client

def build_option_parser(parser):
    return parser
