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

In this SDK, you manipulate Container and Object, all you 
need is to initialize a `ObjectStorageAPI` object.
To initialize it, you need the proxyd url and the namespace name:

    from oiopy.object_storage import ObjectStorageAPI
    s = ObjectStorageAPI("NS", "http://localhost:8000")

All of the sample code that follows assumes that you have correctly initialized
a `ObjectStorageAPI` object.

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

    s.container_create("myaccount", "mycontainer")

Note that you will need to specify an Account name.
    
Note that if you try to create a container more than once with the same name,
the request is ignored

Show a Container
----------------

To show a container:

    info = s.container_show("myaccount", "mycontainer")
    
Note that if you try to get a non-existent container, a `NoSuchContainer`
exception is raised.

Storing Objects
---------------

This example creates an object named `object.txt` with the data provided, in the
container `mycontainer`:

    data = "Content example"
    s.object_create("myaccount", "mycontainer", obj_name="object.txt", 
                        data=data)
  
Note that if you try to store an object in a non-existent container, a
`NoSuchContainer` exception is raised.

Optional Parameters:
*   `metadata` - A dict of metadata to set to the object.
*   `content_length` - If the content length can not be determined from the
provided data source, you must specify it.
*   `content_type` - Indicates the type of file. Examples of `content_type`:
`application/pdf` or `image/jpeg`.

Retrieving Object
-----------------

The methods returns a generator, you must iterate on the generator to retrieve
the content.

Optional Parameters:
*   `size` - Number of bytes to fetch from the object.
*   `offset` - Retrieve the object content from the specified offset.

Note that if you try to retrieve a non-existent object, a `NoSuchObject`
exception is raised.

This sample code stores an object and retrieves it using the different
parameters.

    data = "Content Example"
    s.object_create("myaccount", "mycontainer", obj_name="object.txt", 
                        data=data)

    print "Fetch object"
    meta, stream = s.object_fetch("myaccount", "mycontainer", "object.txt")
    print "".join(stream)

    print
    print "Fetch partial object"
    meta, stream = s.object_fetch("myaccount", "mycontainer", "object.txt", 
                                    offset=8)
    print "".join(stream)


Deleting Objects
----------------

Example:

    s.object_delete("myaccount", "mycontainer", "object.txt")
    
Note that if you try to delete a non-existent object, a `NoSuchObject`
exception is raised.

Containers and Objects Metadata
-------------------------------

The Object Storage API lets you set and retrieve your own metadata on containers
and objects.

    s.container_create("myaccount", "mycontainer")
    meta = s.container_show("myaccount", "mycontainer")
    print "Metadata:", meta
    

It should output and empty dict, unless you added metadata to this container.

    new_meta = {"color": "blue", "flag": "true"}
    s.container_update("myaccount", "mycontainer", new_meta)
    
    meta = s.container_show("myaccount", "mycontainer")
    print "Metadata:", meta
    
It should now output:

    Metadata: {'color': 'a', 'flag': 'true'}
    

This is very similar for objects.
You can use the methods '`object_show()` and `object_update()`.

    
Listing Objects
---------------

    objs = s.object_list("myaccount", "mycontainer")
    
This returns a list of objects stored in the container.

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

    s.container_create("myaccount", "mycontainer")
    for id in range(5):
        s.object_create("myaccount", "mycontainer", obj_name="object%s" % id,
                       data="sample")
    start = ord("a")
    for id in xrange(start, start + 4):
        s.object_create("myaccount", "mycontainer", obj_name="foo/%s" % chr(id), 
                       data="sample")
        
First list all the objects:

    l = s.object_list("myaccount", "mycontainer")
    objs = l['objects']
    for obj in objs:
        print obj['name']

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
    l = s.object_list("myaccount", "mycontainer", limit=limit, marker=marker)
    objs = l['objects']
    print "Objects:", [obj['name'] for obj in objs]
    while objs:
        marker = objs[-1]['name']
        objs = s.object_list("myaccount", "mycontainer", limit=limit, marker=marker)
        print "Objects:" , [obj['name'] for obj in objs]
 
Here is the result:

    Objects: ['foo/a', 'foo/b', 'foo/c', 'foo/d']
    Objects: ['object0', 'object1', 'object2', 'object3']
    Objects: ['object4']
    Objects: []
 
        
How to use the `prefix` parameter:

    l = s.object_list("myaccount", "mycontainer", prefix="foo")
    objs = l['objects']
    print "Objects:", [obj['name'] for obj in objs]
    
This only outputs the objects starting with "foo":

    Objects: ['foo/a', 'foo/b', 'foo/c, 'foo/d']
 

How to use the `delimiter` parameter:
    
    l = s.object_list("myaccount", "mycontainer", delimiter="/")
    objs = l['objects']
    print "Objects:", [obj['name'] for obj in objs]
    
This excludes all the objects in the nested 'foo' folder.

    Objects: ['object0', 'object1', 'object2', 'object3', 'object4']
    
Note that if you try to list a non-existent container, a `NoSuchContainer`
exception is raised.


Deleting Containers
-------------------

There is several options to delete containers.
Example:

    s.container_delete("myaccount", "mycontainer")

You can not delete a container if it still holds objects, if you try to do so
a `ContainerNotEmpty` exception is raised.

Note that if you try to delete a non-existent container, a `NoSuchContainer`
exception is raised.
