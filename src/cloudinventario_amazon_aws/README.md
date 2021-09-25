# Amazon Web Services - AWS

# Config

* access_key
* secret_key
* region
* collect (list of resources to collect, eg.: elb, s3, rds...)

# Collecting for EC2 (always collected, type: vm)

* id
* name
* cluster
* project
* type
* cpus
* memory
* disks
* storage
* primary_ip
* primary_fqdn
* public_ip
* public_fqdn
* networks
* storages
* monitoring
* owner
* os
* status
* is_on
* tags

# Collecting for EBS (type: ebs)

* id
* cluster
* storage
* type
* status
* is_on
* encrypted
* mounts
* details
* tags

# Collecting for ELB (type: elb)

* id
* created
* name
* cluster
* instances
* public_fqdn
* owner
* status
* is_on
* scheme
* subnets
* tags

# Collecting for RDS (type: rds)

* id
* name
* type
* cpus
* memory
* location
* created
* status
* address
* maintenance_window
* encrypted
* public
* instance_class
* storage
* port
* multi_az
* version
* storage_type
* tags

# Collecting for S3 (type: s3)

* id
* name
* location
* ownership_controls
* policy_status
* versioning
* website
* owner
* tags
