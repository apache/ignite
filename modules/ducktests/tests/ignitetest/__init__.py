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

# This determines the version of ignitetest that can be published to PyPi and installed with pip
#
# Note that in development, this version name can't follow Ignite's convention of having a trailing "-SNAPSHOT"
# due to python version naming restrictions, which are enforced by python packaging tools
# (see  https://www.python.org/dev/peps/pep-0440/)
#
__version__ = '2.15.0-SNAPSHOT'
