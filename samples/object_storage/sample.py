from oiopy import object_storage

# Create storage API
storage = object_storage.StorageAPI('http://localhost', 'NS')

# Create a container
container = storage.create("myaccount", "test")

# Get container metadata
metadata = container.get_metadata()

# Create object with string data
obj = storage.store_object("myaccount", container, "object0", "data")

# Create object with file object
with open('test_file', 'rb') as f:
    obj1 = container.create(f, content_length=1024)

# Fetch object content
data = obj.fetch()

# Object metadata
print "Object Details"
print "Name: %s" % obj.name
print "Size: %s" % obj.size
print "Hash: %s" % obj.hash
print "Content Type: %s" % obj.content_type
print "Hash Method: %s" % obj.hash_method
print "Version: %s" % obj.version
print "Policy: %s" % obj.policy

# List container
objs = container.list()

print "Container listing"
for obj in objs:
    print "Object: %s" % obj.name

# Delete objects
obj.delete()
obj1.delete()

# Delete container
container.delete()

