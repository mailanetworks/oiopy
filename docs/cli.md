# Using the command line

To list available commands run `openio help`:

	usage: openio [--version] [-v] [--log-file LOG_FILE] [-q] [-h] [--debug]
	      [--oio-ns <namespace>] [--oio-account <account>]
	      [--oio-proxyd-url <proxyd url>]

	Command-line interface to the OpenIO APIs

	optional arguments:
	--version             show program's version number and exit
	-v, --verbose         Increase verbosity of output. Can be repeated.
	--log-file LOG_FILE   Specify a file to log output. Disabled by default.
	-q, --quiet           suppress output except warnings and errors
	-h, --help            show this help message and exit
	--debug               show tracebacks on errors
	--oio-ns <namespace>  Namespace name (Env: OIO_NS)
	--oio-account <account>
			Account name (Env: OIO_ACCOUNT)
	--oio-proxyd-url <proxyd url>
			Proxyd URL (Env: OIO_PROXYD_URL)

	Commands:
	...

# Environment variables

The following list of environment variables are accepted by the `openio` command line:

* `OIO_NS` The namespace name.
* `OIO_ACCOUNT` The account name to use.
* `OIO_PROXYD_URL` Proxyd URL to connect to.

# Configuration files

By default, the `openio` command line looks for its configuration in
`/etc/oio/sds` and in the directory `.oio` within your `HOME`.


# Help

To get help on any command, just execute the command with the `--help` option.

	$ openio container create --help
	container create [-h] [-f {csv,html,json,table,value,yaml}]
			       [-c COLUMN] [--max-width <integer>]
			       [--quote {all,minimal,none,nonnumeric}]
			       <container-name> [<container-name> ...]

	Create container

	positional arguments:
	<container-name>      New container name(s)
	...


# Get started

You need to initialize the namespace and account environment variables before using the CLI:

	$ export OIO_NS=OPENIO
	$ export OIO_ACCOUNT=myaccount
