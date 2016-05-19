import logging
import pkg_resources

try:
    __version__ = __canonical_version__ = pkg_resources.get_provider(
        pkg_resources.Requirement.parse('oiopy')).version
except pkg_resources.DistributionNotFound:

    import pbr.version
    _version_info = pbr.version.VersionInfo('oiopy')
    __version__ = _version_info.release_string()
    __canonical_version__ = _version_info.version_string()


def set_logger(name='oiopy', level=logging.DEBUG, fmt=None):
    if fmt is None:
        fmt = '%(asctime)s %(name)s [%(levelname)s] %(message)s'
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class NullHandler(logging.Handler):
    def emit(self, r):
        pass


logging.getLogger('oiopy').addHandler(NullHandler())
