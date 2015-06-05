Object Storage
==============

Basic Concepts
--------------

An Object Storage API differs from a conventional filesystem: instead of
directories and files, you manipulate containers where you store objects. A
container can hold millions of objects. 

Note that there is no hierarchy notion with containers: you cannot nest a
container within an other, however you can emulate a nested folder structure
with a naming convention for your objects.
For example with an object name such as "documents/work/2015/finance/report.pdf"
you can retrieve your files using the appropriate "path" prefix.

In this SDK, you manipulate `Container` and `StorageObject` classes, all you 
need is to initialize a `StorageAPI` object.
To initialize it, you need the proxyd url and the namespace name:

    from oiopy import object_storage
    s = object_storage.StorageAPI("http://localhost:8000", "NS")

All of the sample code that follows assumes that you have correctly initialized
a `StorageAPI` object.

Accounts
--------
Accounts are a convenient way to manage the storage containers. Containers 
always belong to a specific Account.

You can list containers for a specified Account.
Accounts are also a great way to track your storage usage (Total bytes used, 
Total number of objects, Total number of containers).

The API lets you set and retrieve your own metadata on accounts.


Creating a Container
--------------------

Start by creating a container:

    container = s.create("myaccount", "example")
    print "Name:", container.name
    print "Total size:", container.total_size
    

Note that you will need to specify an Account name.

It should output:

    Name: example
    Total size: 0
    
Note that if you try to create a container more than once with the same name,
the request is ignored and a reference to the existing container is returned.

Getting a Container
-------------------

To get a `Container`:

    container = s.get("myaccount", "example")
    print "Container:", container

It should output:

    Container: <Container 'example'>
    
Note that if you try to get a non-existent container, a `NoSuchContainer`
exception is raised.

Storing Objects
---------------

There is two options to store objects: passing directly the content with
`store_object()`, or passing in a file-like object with `upload_file()`.

Note that if you try to store an object in a non-existent container, a
`NoSuchContainer` exception is raised.

This example creates an object named `object.txt` with the data provided, in the
container `example`:

    data = "Content example"
    obj = s.store_object("myaccount", "example", "object.txt", data)
    print "Object:", obj


It should output:

    Object: <Object 'object.txt'>
    
Note that for methods that take a container as a parameter, you can pass
directly the container instance:

    container = s.create("myaccount", "example")
    obj = s.store_object(container, "object", "sample")
 

The example shows how you can use the `upload_file()` method:

    path = "/home/me/foo/file.txt"
    print "Upload File"
    obj = s.upload_file("myaccount", "example", path)
    print "Stored Object:", obj
    
    print
    print "Upload File-like Object"
    with open("file.txt", "rb") as f:
        obj1 = s.upload_file("myaccount", "example", f)
    print "Stored Object:", obj1
        
The methods return a `StorageObject` instance.
If you have a `Container` instance, you can also call these methods
directly.

Optional Parameters:
*   `metadata` - A dict of metadata to set to the object.
*   `content_length` - If the content length can not be determined from the
provided data source, you must specify it.
*   `content_type` - Indicates the type of file. Examples of `content_type`:
`application/pdf` or `image/jpeg`.

Retrieving Object
-----------------

There is several options to retrieve objects.
If you have the `Object` reference, just use its `fetch()` method.
If you have the `Container` object that holds the object, use its
`fetch_object()` method.
Also you can use the method `fetch_object()` of `StorageAPI`, where you must
specify the container and object names.

The methods returns a generator, you must iterate on the generator to retrieve
the content.

Optional Parameters:
*   `with_meta` - If True, the method returns a 2-tuple, the first element
contains the object metadata and the second element is the generator.
*   `size` - Number of bytes to fetch from the object.
*   `offset` - Retrieve the object content from the specified offset.

Note that if you try to retrieve a non-existent object, a `NoSuchObject`
exception is raised.

This sample code stores an object and retrieves it using the different
parameters.

    data = "Content Example"
    obj = s.store_object("myaccount", "example", "object.txt", data)

    print "Fetch object"
    gen = obj.fetch()
    print "".join(gen)

    meta, gen = obj.fetch(with_meta=True)
    print
    print "Metadata:", meta

    print
    print "Fetch partial object"
    gen = obj.fetch(offset=8)
    print "".join(gen)


Deleting Objects
----------------

There is several options to delete objects.
If you have the `Object` reference, just use its `delete()` method.
If you have the `Container` object that holds the object, use its
`delete_object()` method.
Also you can use the method `delete_object()` of `StorageAPI`, where you must
specify the container and object names.

Note that if you try to delete a non-existent object, a `NoSuchObject`
exception is raised.

Containers and Objects Metadata
-------------------------------

The Object Storage API lets you set and retrieve your own metadata on containers
and objects.

    container = s.create("myaccount", "example")
    meta = s.get_container_metadata("myaccount", container)
    print "Metadata:", meta
    

It should output and empty dict, unless you added metadata to this container.

    new_meta = {"color": "blue", "flag": "true"}
    s.set_container_metadata("myaccount", container, new_meta)
    
    meta = s.get_container_metadata("myaccount", container)
    print "Metadata:", meta
    
It should now output:

    Metadata: {'color': 'a', 'flag': 'true'}
    
There is several options to get and set metadata to containers.
You can use the methods `get_container_metadata()` and 
`set_container_metadata()` of `StorageAPI`.
If you have a `Container` instance you can also use its methods `get_metadata()`
and `set_metadata()`.

This is very similar for objects.
You can use the methods '`get_object_metadata()` and `set_object_metadata()` of
`StorageAPI` or `Container`.
If you have `StorageObject` instance you can also use its methods
`get_metadata()` and `set_metadata()`.

    
Listing Objects
---------------

If you have a `Container` instance:

    objs = container.list()
    
This returns a list of `StorageObject` stored in the container.

Since containers can hold millions of objects, there are several methods to
filter the results.

Filters:
*   `marker` - Indicates where to start the listing from.
*   `end_marker` - Indicates where to stop the listing.
*   `prefix` - If set, the listing only includes objects whose name begin with
its value.
*   `delimiter` - If set, excludes the objects whose name contains its value.
`delimiter` only takes a single character.
*   `limit` - Indicates the maximum number of objects to return in the listing.


To illustrate these features, we create some objects in a container:

    container = s.create("myaccount", "example")
    for id in range(5):
        s.store_object("myaccount", container, "object%s" % id, "sample")
    start = ord("a")
    for id in xrange(start, start + 4):
        s.store_object("myaccount", container, "foo/%s" % chr(id), "sample")
        
First list all the objects:

    objs = container.list()
    for obj in objs:
        print obj.name

It should output:
    
    foo/a
    foo/b
    foo/c
    foo/d
    object0
    object1
    object2
    object3
    object4
    
Then let's use the paginating features:

    limit = 4
    marker = ""
    objs = container.list(limit=limit, marker=marker)
    print "Objects:", [obj.name for obj in objs]
    while objs:
        marker = objs[-1].name
        objs = container.list(limit=limit, marker=marker)
        print "Objects:" , [obj.name for obj in objs]
 
Here is the result:

    Objects: ['foo/a', 'foo/b', 'foo/c', 'foo/d']
    Objects: ['object0', 'object1', 'object2', 'object3']
    Objects: ['object4']
    Objects: []
 
        
How to use the `prefix` parameter:

    objs = container.list(prefix="foo")
    print "Objects:", [obj.name for obj in objs]
    
This only outputs the objects starting with "foo":

    Objects: ['foo/a', 'foo/b', 'foo/c, 'foo/d']
 

How to use the `delimiter` parameter:
    
    objs = container.list(delimiter="/")
    print "Objects:", [obj.name for obj in objs]
    
This excludes all the objects in the nested 'foo' folder.

    Objects: ['object0', 'object1', 'object2', 'object3', 'object4']
    
Note that if you try to list a non-existent container, a `NoSuchContainer`
exception is raised.


Deleting Containers
-------------------

There is several options to delete containers.
If you have a `Container` instance:

    container.delete()
    
Also you can use the method `delete()` of `StorageAPI` where you must specify
the container.

You can not delete a container if it still holds objects, if you try to do so
a `ContainerNotEmpty` exception is raised.

Note that if you try to delete a non-existent container, a `NoSuchContainer`
exception is raised.
