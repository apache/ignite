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
Deliberately empty - pytest imports this file for its side effect alone, which is to put the
directory it sits in on ``sys.path``.

That is the whole of it: importing a conftest adds *its own* directory, and pytest on its own
adds only each check file's. Two things depend on this one being the directory that gets
added, so moving this file down into ``checks/`` - which is where it looks like it belongs -
breaks both:

    - ``from checks.support... import ...``. The checks live in ``checks/``, their helper
      modules apart from them in ``checks/support/``, and the name ``checks`` resolves only
      while its parent is on the path. A conftest in ``checks/`` would add ``checks/`` and the
      import would have to become a bare ``support...`` instead.

    - ``import ignitetest``, under tox. ``[tox] skipsdist = True`` reads to tox 4 as
      ``no_package``, which silently drops the ``usedevelop`` install, so the tox environment
      has no installed ignitetest at all and reaches the checkout through this path entry
      alone. Take this file away and every check file fails at collection, not just the ones
      importing ``checks.support``. Outside tox the editable install covers it, which is why
      the breakage only shows in one of the two ways the checks are run.

The second reason is a workaround, not a design: once the packaging is fixed - a
``pyproject.toml`` and a PEP 517 editable build, so ``usedevelop`` works again - only the
first remains, and this file could then reasonably move into ``checks/``.

Nothing below ``checks/`` carries an ``__init__.py`` on purpose: ``setup.py`` collects the
distribution with ``find_packages()``, which only finds directories that have one, so the
checks and their helpers stay out of the installed ``ignitetest`` package without ``setup.py``
having to name them, and ``checks.support`` resolves as a namespace package instead.
"""
