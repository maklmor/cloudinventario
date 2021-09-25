# Amazon Lightsail

# Config

* access_key
* secret_key
* region
* collect (list of resources to collect, eg.: db, lb...)

# Collecting for Lightsail (always collected, type: lightsail)

* id
* name
* created
* cluster
* location
* type
* cpus
* memory
* disks
* storage
* primary_ip
* public_ip
* storages
* owner
* blueprint_name
* status
* is_on
* tags

# Collecting for Relational Databases (type: db)

* id
* name
* type
* cpus
* memory
* created
* cluster
* location
* storage
* version
* owner
* status
* port
* address
* public
* tags

# Collecting for Disks (type: disk)

* id
* created
* name
* cluster
* location
* storage
* is_system_disk
* path
* owner
* status
* iops
* is_on
* tags

# Collecting for Load Balancers (type: lb)

* id
* created
* name
* cluster
* location
* instances
* dns_name
* owner
* status
* is_on
* tags
