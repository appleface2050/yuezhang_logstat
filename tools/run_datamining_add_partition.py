import pexpect
import os
import sys

ssh = pexpect.spawn('ssh datamining@192.168.0.150')
ssh.expect('.ssword:*')
ssh.sendline('admin1234!')
#run_sh = pexpect.spawn('sh /home/datamining/shangzongkai/add_partition_basic_service_v6_simp.sh')
run_sh = pexpect.run('sh /home/datamining/shangzongkai/add_partition_basic_service_v6_simp.sh')
ssh.interact()
