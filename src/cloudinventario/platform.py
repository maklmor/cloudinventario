import re

re_linux = re.compile(".*Linux|Ubuntu|Debian|CentOS|RedHat|Alpine|Gentoo|ROCK", re.IGNORECASE)
OS_LINUX = "Linux"
re_routeros = re.compile(".*RouterOS", re.IGNORECASE)
OS_ROUTEROS = "RouterOS"
re_windows = re.compile(".*Windows", re.IGNORECASE)
OS_WINDOWS = "Windows"
re_vmware = re.compile(".*VMware", re.IGNORECASE)
OS_VMWARE = "VMware"
re_cisco = re.compile(".*Cisco", re.IGNORECASE)
OS_CISCO = "Cisco"
OS_OTHER = "Other"

def get_os_family(str, desc = None):
  if re_linux.match(str):
    if desc and re_routeros.match(desc):
      return OS_ROUTEROS
    return OS_LINUX
  elif re_windows.match(str):
    return OS_WINDOWS
  elif re_routeros.match(str):
    return OS_ROUTEROS
  elif re_vmware.match(str):
    return OS_VMWARE
  elif re_cisco.match(str):
    return OS_CISCO
  else:
    return OS_OTHER

def get_os(str, desc = None):
  if re_linux.match(str) and desc and re_routeros.match(desc):
      return "RouterOS/Linux"
  return str
