# VMWare VSphere

# Config

* host
* port (default: 443)
* user
* pass

# Collecting for Clusters (type: cluster)

* id
* name
* cpus
* memory

# Collecting for Servers (type: server)

* id
* name
* project (cluster)
* cpus
* memory
* storage
* primary_ip
* management_ip
* networks
* os
* status
* is_on

# Collecting for Servers NICs (networks)

* name
* mac
* ip
* subnet

# Collecting for VApps (type: vapp)

* id
* name
* memory

# Collecting for VMs (type: vm)

VM data are repoted by VM guest-agent.

* id
* name
* project (VApp)
* cluster
* description
* cpus
* memory
* storage
* storages
* disks
* primary_ip
* networks
* os
* status
* is_on
* template

# Collecting for VM Disks (storages)

* id
* name
* capacity
* profile
* thin

# Collecting for VM NICs (networks)

* id
* mac
* network
* ip
* aliases
* primary
* connected
