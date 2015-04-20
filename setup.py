from setuptools import setup

from oiopy import __version__


setup(
    name='oiopy',
    version=__version__,
    description='OpenIO SDS Python API',
    author='OpenIO',
    author_email='support@openio.io',
    url='https://github.com/open-io/oiopy',
    license='LGPLv3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
    ],
    packages=['oiopy'],
    install_requires=[
        'eventlet', 'urllib3'
    ]

)
