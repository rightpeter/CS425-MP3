import os

FILENAME=""
VMNAME=""

def send_file_to_sdfs(file_name, vm_name):
    os.system("scp ./{0} {1}:/tmp/".format(file_name, vm_name))



print """
1) Place the wikipedia corpus in the directory from which this script is run.

2) Edit FILENAME to the name of the corpus

3) Edit VMNAME to the name of the VM you want to send file to
"""

send_file_to_sdfs(FILENAME, VMNAME)