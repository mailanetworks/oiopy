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
    [...]

### Environment variables

The following list of environment variables are accepted by the `openio` command
 line:

* `OIO_NS` The namespace name.
* `OIO_ACCOUNT` The account name to use.
* `OIO_PROXYD_URL` Proxyd URL to connect to.

# Configuration files

By default, the `openio` command line looks for its configuration in
`/etc/oio/sds` and in the directory `.oio` within your `HOME`.

### Help

To get help on any command, just execute the command with the `--help` option.

    # openio container create --help

    usage : openio container create [-h] [-f {csv,html,json,table,value,yaml}]
                   [-c COLUMN] [--max-width <integer>]
                   [--quote {all,minimal,none,nonnumeric}]
                   <container-name> [<container-name> ...]

    Create container

    positional arguments:
    <container-name>      New container name(s)
    [...]

### Get started

You need to initialize the namespace and account environment variables before using the CLI. Adapt the OIO_NS value to yours:

    # export OIO_NS=OPENIO
    # export OIO_ACCOUNT=my_account

## Container management

### Create a container

    # openio container create my_container

    +--------------+---------+
    | Name         | Created |
    +--------------+---------+
    | my_container | True    |
    +--------------+---------+

### Information about container

Display information about this container.

    # openio container show my_container

    +-------------+--------------------------------------------------------------------+
    | Field       | Value                                                              |
    +-------------+--------------------------------------------------------------------+
    | account     | my_account                                                         |
    | base_name   | CB2D04216603B8274AB831F889EAA4B2656D1EBA45B658712D59C77DAC86E08A.1 |
    | bytes_usage | 0                                                                  |
    | container   | my_container                                                       |
    | ctime       | 1441105114                                                         |
    +-------------+--------------------------------------------------------------------+

You can't display informations about a non-existent container

### Container properties

You can manually set a property <key=value> to a given container. If the property set that way is new, its key must begin with a `user.` prefix:

    # openio container set my_container --property user.nbcontents=20

The property key set this way is saved with the `meta.` prefix:

    # openio container show my_container

    +-----------------+--------------------------------------------------------------------+
    | Field           | Value                                                              |
    +-----------------+--------------------------------------------------------------------+
    [...]
    | meta.nbcontents | 20                                                                 |
    +-----------------+--------------------------------------------------------------------+

You can also delete a property by specifying its key and using the `user.` prefix again :

    # openio container unset my_container --property user.nbcontents

## Object management

### Store object

    # echo 'Hello OpenIO!' > test.txt
    # openio object create my_container test.txt

    +----------+------+----------------------------------+
    | Name     | Size | Hash                             |
    +----------+------+----------------------------------+
    | test.txt |   14 | 9eb03b6e836ceae565ba79f76c821dda |
    +----------+------+----------------------------------+

### List objects

    [openio@openio ~]# openio object list my_container

    +----------+------+----------------------------------+
    | Name     | Size | Hash                             |
    +----------+------+----------------------------------+
    | test.txt |   14 | 9EB03B6E836CEAE565BA79F76C821DDA |
    +----------+------+----------------------------------+

Since containers can hold millions of objects, there are several methods to
filter the results.

Here are the optional arguments which can be used:
*   `--marker` : Indicates where to start the listing from.
*   `--end_marker` : Indicates where to stop the listing.
*   `--prefix` : If set, the listing only includes objects whose name begin with
its value.
*   `--delimiter` : If set, excludes the objects whose name contains its value.
`delimiter` only takes a single character. It can also be combined with the `prefix` argument for more precise listings.
*   `--limit` : Indicates the maximum number of objects to return in the listing.

To illustrate these features, create those files and store them in a container

    # openio touch folder_1 folder_2 folder_3_0 file1 file2 config1 config2
    # openio object create my_container folder_1 folder_2 folder_3_0 file1 file2 config1 config2
    [...]

You can list all the objects :

    # openio object list my_container

    +------------+------+----------------------------------+
    | Name       | Size | Hash                             |
    +------------+------+----------------------------------+
    | config1    |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | config2    |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | file1      |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | file2      |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | folder_1   |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | folder_2   |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | folder_3_0 |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | test.txt   |   14 | 9EB03B6E836CEAE565BA79F76C821DDA |
    +------------+------+----------------------------------+

Let's use the paginating features:

    # openio object list my_container --limit 3 --marker e

This only outputs a maximum of three objects whose names are lexically greater than `e`:

    +----------+------+----------------------------------+
    | Name     | Size | Hash                             |
    +----------+------+----------------------------------+
    | file1    |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | file2    |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | folder_1 |    0 | D41D8CD98F00B204E9800998ECF8427E |
    +----------+------+----------------------------------+

    # openio object list my_container --prefix file

This only outputs the objects whose names begin with `file`:

    +-------+------+----------------------------------+
    | Name  | Size | Hash                             |
    +-------+------+----------------------------------+
    | file1 |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | file2 |    0 | D41D8CD98F00B204E9800998ECF8427E |
    +-------+------+----------------------------------+

    # openio object list my_container --delimiter _

This excludes all the objects whose names contain a `_` character:

    +----------+------+----------------------------------+
    | Name     | Size | Hash                             |
    +----------+------+----------------------------------+
    | config1  |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | config2  |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | file1    |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | file2    |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | test.txt |   14 | 9EB03B6E836CEAE565BA79F76C821DDA |
    +----------+------+----------------------------------+

    # openio object list my_container --prefix folder_ --delimiter _

This collects all the objects whose names begin with `folder_`, and then excludes all those whose names contain a `_` character after the prefix:

    +----------+------+----------------------------------+
    | Name     | Size | Hash                             |
    +----------+------+----------------------------------+
    | folder_1 |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | folder_2 |    0 | D41D8CD98F00B204E9800998ECF8427E |
    +----------+------+----------------------------------+

### Get object

Exports the data stored in the given object to the --file destination :

    # openio object save my_container test.txt --file /tmp/test.txt
    # ls /tmp/test.txt

    /tmp/test.txt

If the destination file already exists, its content will be deleted and replaced with this new one.

You can also export all the objects from a container to your working directory in a single command:

    # mkdir test_folder && cd test_folder
    # openio container save my_container
    # ls

    config1  config2  file1  file2  folder_1  folder_2  folder_3_0  test.txt

    # cd .. && rm -rf test_folder

### Informations about object

Display the different services involved by this content, including policy, MD5, properties …

    # openio object show my_container test.txt

    +-----------+----------------------------------+
    | Field     | Value                            |
    +-----------+----------------------------------+
    | account   | my_account                       |
    | container | my_container                     |
    | ctime     | 1441057689                       |
    | hash      | 9eb03b6e836ceae565ba79f76c821dda |
    | mime-type | octet/stream                     |
    | object    | test.txt                         |
    | policy    | none                             |
    | size      | 14                               |
    +-----------+----------------------------------+

You can manually set a property <key=value> to a given object:

    # openio object set my_container test.txt --property size=small

The property key set this way is saved with the `meta.` prefix:

    # openio object show my_container test.txt

    +-----------+----------------------------------+
    | Field     | Value                            |
    +-----------+----------------------------------+
    [...]
    | meta.size | small                            |
    [...]
    +-----------+----------------------------------+

You can also delete a property by specifying its key:

    # openio object unset my_container test.txt --property size

### Delete object

    # openio object delete my_container test.txt

### Destroy container

    # openio container delete my_container

### Note : It is not possible to delete a non empty container.

Indeed, the previous command raised the following error :

    M2 error: HOOK error: Request error: 1 elements still in container (HTTP 409) (STATUS 438)

    # openio object list my_container

    +------------+------+----------------------------------+
    | Name       | Size | Hash                             |
    +------------+------+----------------------------------+
    | config1    |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | config2    |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | file1      |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | file2      |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | folder_1   |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | folder_2   |    0 | D41D8CD98F00B204E9800998ECF8427E |
    | folder_3_0 |    0 | D41D8CD98F00B204E9800998ECF8427E |
    +------------+------+----------------------------------+

There are still 7 objects in the container.

To handle that situation, you need first to delete all objects stored in the container.

    # openio object delete my_container folder_3_0 folder_2 folder_1 file2 file1 config2 config1

    # openio object list my_container

And finally destroy the container.

    # openio container delete my_container

# Advanced usages

## Account management

Accounts are a convenient way to manage the storage containers. Containers always belong to a specific Account.

### Create an account

    # openio account create my_account_2

You can now specify in which account to execute the actions by adding the `--oio-account <account_name>` parameter to your commands:

    # openio container create test_container --oio-account my_account_2

    +----------------+---------+
    | Name           | Created |
    +----------------+---------+
    | test_container | True    |
    +----------------+---------+

    # openio container list --oio-account my_account

There is no container named `test_container` in the account `my_account`

    # openio container list --oio-account my_account_2

    +----------------+-------+-------+
    | Name           | Bytes | Count |
    +----------------+-------+-------+
    | test_container |     0 |     0 |
    +----------------+-------+-------+

### Information about account

    # openio account show my_account_2

    +------------+------------------+
    | Field      | Value            |
    +------------+------------------+
    | bytes      | 0                |
    | containers | 1                |
    | ctime      | 1441108158.46772 |
    | id         | my_account_2     |
    | metadata   | {}               |
    | objects    | 0                |
    +------------+------------------+

## References

References are also a convenient way to manage the storage containers. Containers always belong to a specific reference.

To manage containers, a reference must be linked to one of the following service type : meta0, meta1, meta2 and rawx.

All along that tutorial, each time you created a container, a corresponding reference was also created and linked with a service.

### Create a reference

You can manually create a clean unlinked reference :

    # openio reference create my_reference

### Link a reference

    # openio reference link my_reference meta2

You can link a reference with any amount of services as long as a service type contains unlinked services.

### Information about reference

    # openio reference show my_reference

    +---------+--------------+
    | Field   | Value        |
    +---------+--------------+
    | account | my_account   |
    | name    | my_reference |
    +---------+--------------+

You can also list all the services linked with the reference

    # openio reference link my_reference meta1
    # openio reference link my_reference rawx
    # openio reference list my_reference

    +-------+---------------------+------+-----+
    | Type  | Host                | Args | Seq |
    +-------+---------------------+------+-----+
    | rawx  | 192.168.56.101:6011 |      |   1 |
    | meta2 | 192.168.56.101:6008 |      |   1 |
    | meta1 | 192.168.56.101:6007 |      |   1 |
    +-------+---------------------+------+-----+

### Unlink a reference

    # openio reference unlink my_reference meta2
    
All the meta2 services are dissociated from the reference:

    # openio reference list my_reference

    +-------+---------------------+------+-----+
    | Type  | Host                | Args | Seq |
    +-------+---------------------+------+-----+
    | rawx  | 192.168.56.101:6011 |      |   1 |
    | meta1 | 192.168.56.101:6007 |      |   1 |
    +-------+---------------------+------+-----+

### Delete a reference

    # openio reference delete my_reference

It is not possible to delete a linked reference.
Indeed, the previous instruction raised the following answer :
 
    META1 error: Request error: Query error: User still linked to services (HTTP 500) (STATUS 407)

    # openio reference list my_reference

    +-------+---------------------+------+-----+
    | Type  | Host                | Args | Seq |
    +-------+---------------------+------+-----+
    | rawx  | 192.168.56.101:6011 |      |   1 |
    | meta1 | 192.168.56.101:6007 |      |   1 |
    +-------+---------------------+------+-----+

There are still two services linked with the reference.
First you need to unlink all of it.

    # openio reference unlink my_reference rawx
    # openio reference unlink my_reference meta1

And finally destroy the reference.

    # openio reference delete my_reference
