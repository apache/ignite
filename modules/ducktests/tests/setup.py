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

import re
from setuptools import find_packages, setup


with open('ignitetest/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)


# Note: when changing the version of ducktape, also revise tests/docker/Dockerfile
setup(name="ignitetest",
      version=version.replace("-SNAPSHOT", ".dev0"),
      description="Apache Ignite System Tests",
      author="Apache Ignite",
      platforms=["any"],
      license="apache2.0",
      packages=find_packages(exclude=["ignitetest.tests", "ignitetest.tests.*"]),
      include_package_data=True,
      install_requires=open('docker/requirements.txt').read(),
      tests_require=["pytest==6.2.5"])
