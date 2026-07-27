# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Table-driven checks over recorded OpenSSH stderr samples."""

import pytest

from ducktests_remote import sshdiag

SAMPLES = [
    (sshdiag.UNRESOLVED,
     "ssh: Could not resolve hostname node07.dc.local: Name or service not known"),
    (sshdiag.UNRESOLVED,
     "ssh: Could not resolve hostname host: nodename nor servname provided, or not known"),
    (sshdiag.UNRESOLVED,
     "ssh: Could not resolve hostname x: Temporary failure in name resolution"),
    (sshdiag.NO_SSHD,
     "ssh: connect to host node03.dc.local port 22: Connection refused"),
    (sshdiag.UNREACHABLE,
     "ssh: connect to host node04.dc.local port 22: Connection timed out"),
    (sshdiag.UNREACHABLE,
     "ssh: connect to host node05 port 22: No route to host"),
    (sshdiag.UNREACHABLE,
     "ssh: connect to host node06 port 22: Network is unreachable"),
    (sshdiag.NO_ACCESS,
     "max@node02.dc.local: Permission denied (publickey,gssapi-keyex,password)."),
    (sshdiag.NO_ACCESS,
     "Received disconnect from 10.0.0.5 port 22:2: Too many authentication failures"),
    (sshdiag.NO_USER,
     "Invalid user max from 10.0.0.9 port 51234"),
    (sshdiag.NO_USER,
     "Please login as the user \"ec2-user\" rather than the user \"root\"."),
    (sshdiag.HOSTKEY,
     "@@@ WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED! @@@"),
    (sshdiag.HOSTKEY,
     "Host key verification failed."),
    (sshdiag.NO_SUDO,
     "sudo: a password is required"),
    (sshdiag.NO_SUDO,
     "sudo: no tty present and no askpass program specified"),
    (sshdiag.NO_SUDO,
     "max is not in the sudoers file.  This incident will be reported."),
    (sshdiag.UNKNOWN,
     "some entirely novel failure nobody has seen before"),
]


class CheckClassification:
    """Every recorded sample maps to exactly one class."""

    @pytest.mark.parametrize("expected,stderr", SAMPLES)
    def check_sample(self, expected, stderr):
        assert sshdiag.classify(255, stderr) == expected

    def check_success_is_ok_regardless_of_stderr(self):
        assert sshdiag.classify(0, "Warning: Permanently added 'x' to known hosts.") \
            == sshdiag.OK

    def check_advice_is_host_and_user_specific(self):
        diagnosis = sshdiag.SshDiagnosis("node02", sshdiag.NO_ACCESS, user="max", port=2222)
        assert "max" in diagnosis.advice

    def check_unreachable_advice_names_the_port(self):
        diagnosis = sshdiag.SshDiagnosis("node02", sshdiag.UNREACHABLE, user="max", port=2222)
        assert "2222" in diagnosis.advice


class CheckMixedCluster:
    """A partially working cluster is the normal case, not an edge case."""

    def _diagnoses(self):
        return ([sshdiag.SshDiagnosis("node0%d" % i, sshdiag.OK, user="max")
                 for i in range(1, 10)]
                + [sshdiag.SshDiagnosis("node10", sshdiag.NO_ACCESS, user="max"),
                   sshdiag.SshDiagnosis("node11", sshdiag.NO_ACCESS, user="max"),
                   sshdiag.SshDiagnosis("node12", sshdiag.NO_USER, user="max")])

    def check_summary_counts_every_class(self):
        assert sshdiag.summarise(self._diagnoses()) == "9 ok, 2 no-access, 1 no-user"

    def check_admin_block_names_the_right_hosts_per_class(self):
        block = sshdiag.admin_request_block(self._diagnoses(), user="max")
        no_access = block.split("does not exist")[0]
        assert "node10" in no_access and "node11" in no_access
        assert "node12" not in no_access, "a no-user host must not be listed as no-access"
        assert "node12" in block
        no_user = block.split("does not exist")[1]
        assert "node10" not in no_user.split("it does exist on")[0], \
            "a no-access host must not be listed as no-user"

    def check_admin_block_mentions_the_account_and_the_authorized_keys_line(self):
        block = sshdiag.admin_request_block(self._diagnoses(), user="max")
        assert "'max'" in block
        assert "authorized_keys" in block

    def check_admin_block_lists_hosts_that_do_have_the_account(self):
        block = sshdiag.admin_request_block(self._diagnoses(), user="max")
        assert "it does exist on" in block

    def check_no_block_when_everything_works(self):
        healthy = [sshdiag.SshDiagnosis("node01", sshdiag.OK, user="max")]
        assert sshdiag.admin_request_block(healthy, user="max") == ""

    def check_sudo_block_names_the_tests_that_need_it(self):
        block = sshdiag.admin_request_block(
            [sshdiag.SshDiagnosis("node01", sshdiag.NO_SUDO, user="max")], user="max")
        assert "discovery_test.py" in block and "cellular_affinity_test.py" in block

    def check_hostkey_block_never_removes_anything_itself(self):
        block = sshdiag.admin_request_block(
            [sshdiag.SshDiagnosis("node01", sshdiag.HOSTKEY, user="max")], user="max")
        assert "ssh-keygen -R node01" in block
        assert "verify before removing" in block
