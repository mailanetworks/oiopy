from oiopy import object_storage

# Create storage API
storage = object_storage.ObjectStorageAPI("NS", "http://localhost")

# Create a container
container = storage.container_create("myaccount", "test")

# Get container metadata
info = storage.container_show("myaccount", "test")

# Create object with string data
obj = storage.object_create("myaccount", container, obj_name="object0",
                            data="data")

# Create object with file object
with open('test_file', 'rb') as f:
    obj1 = storage.object_create("myaccount", "test", f, content_length=1024)

# Fetch object content
meta, stream = storage.object_fetch("myaccount", "test", "test_file")

# List container
l = storage.object_list("myaccount", "test")

print "Container listing"
for obj in l["listing"]:
    print "Object: %s" % obj

# Delete objects
storage.object_delete("myaccount", "test", "test_file")
storage.object_delete("myaccount", "test", "object0")


# Delete container
storage.container_delete("myaccount", "test")


