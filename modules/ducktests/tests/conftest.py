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
Deliberately empty - pytest imports this file for its side effect alone.

The framework checks live in ``checks/``, whose helper modules sit apart from the check files
themselves (``checks/support/``), and pytest on its own puts only each check file's own
directory on ``sys.path`` - never this one, which is what ``checks.support`` has to be reached
through.

Importing a conftest is what adds its directory, so this file is what makes
``from checks.support... import ...`` resolve, as a namespace package. Nothing below ``checks/``
carries an ``__init__.py`` on purpose: ``setup.py`` collects the distribution with
``find_packages()``, which only finds directories that have one, so the checks and their
helpers stay out of the installed ``ignitetest`` package without ``setup.py`` having to name
them.
"""
