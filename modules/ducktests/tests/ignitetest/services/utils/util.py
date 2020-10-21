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

"""
This module contains utility methods.
"""
import os

from ignitetest.services.utils.ignite_persistence import PersistenceAware


def compress_cmd(src_path, dest_tgz):
    """
    Return a bash command that compresses the given path into a tgz archive.
    """
    return f'cd {src_path} ; tar czf "{dest_tgz}" *;'


def move_file_to_logs(node, file_path: str):
    """
    Move file to logs directory.
    :return new path to file.
    """
    node.account.ssh_output(f'mv {file_path} {PersistenceAware.PATH_TO_LOGS_DIR}')

    file_name = os.path.basename(file_path)

    return os.path.join(PersistenceAware.PATH_TO_LOGS_DIR, file_name)
