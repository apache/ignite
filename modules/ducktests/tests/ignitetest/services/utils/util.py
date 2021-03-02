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


def copy_file_to_dest(node, file_path: str, dest_dir: str):
    """
    Copy file to destination directory.
    :return new path to file.
    """
    node.account.ssh_output(f'cp {file_path} {dest_dir}')

    file_name = os.path.basename(file_path)

    return os.path.join(dest_dir, file_name)
