from oiopy import directory

# Create directory API
directory = directory.DirectoryAPI('http://localhost', 'NS')

# Check has reference
print directory.has("test")

# Create reference
reference = directory.create("test")

# Set properties to reference
reference.set_properties({"a": "1", "b": "2"})

# Get properties
print reference.get_properties()

# Delete properties
reference.delete_properties(["a"])

# Link service to reference
reference.link("meta2")

# List services associated to reference
print reference.list("meta2")

# Unlink service to reference
reference.unlink("meta2")

# Delete reference
reference.delete()
