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

import os.path

from collections import namedtuple

def java_version(node):
    # Determine java version on the node
    version = -1
    for line in node.account.ssh_capture("java -version"):
        if line.find("version") != -1:
            version = parse_version_str(line)
    return version

def parse_version_str(line):
    # Parse java version string. Examples:
    #`openjdk version "11.0.5" 2019-10-15` will return 11.
    #`java version "1.5.0"` will return 5.
    line = line[line.find('version \"') + 9:]
    dot_pos = line.find(".")
    if line[:dot_pos] == "1":
        return int(line[dot_pos+1:line.find(".", dot_pos+1)])
    else:
        return int(line[:dot_pos])
