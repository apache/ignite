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

"""A recording Transport used by the unit checks. No processes, no network."""

import posixpath

from ducktests_remote.transport import Result, Transport


class FakeTransport(Transport):
    """
    Records every command and simulates a small filesystem.

    Responses are registered as ``(predicate, Result)`` pairs; anything unmatched
    succeeds with empty output, which keeps the checks focused on what they assert.
    """

    def __init__(self, name="fake", home="/home/tester", **kw):
        super().__init__(name=name, **kw)
        self.commands = []
        self.scripts = []
        self.uploads = []
        self.downloads = []
        self.dirs = []
        self.files = {}
        self.responses = []
        self._home = home

    # -- registration ------------------------------------------------------------

    def when(self, needle, stdout="", returncode=0, stderr=""):
        """Reply to any command whose joined argv contains ``needle``."""
        self.responses.append((needle, Result([], returncode, stdout, stderr, self.name)))
        return self

    # -- Transport ---------------------------------------------------------------

    def run(self, argv, *, check=True, timeout=None, input=None):  # noqa: A002
        argv = [str(a) for a in argv]
        self.commands.append(argv)
        if input is not None:
            self.scripts.append(input if isinstance(input, str) else input.decode("utf-8"))
        joined = " ".join(argv)
        for needle, canned in self.responses:
            if needle in joined or (input and needle in str(input)):
                result = Result(argv, canned.returncode, canned.stdout, canned.stderr, self.name)
                return result.check() if check and canned.returncode else result
        return Result(argv, 0, "", "", self.name)

    def upload(self, local_path, remote_path, *, mode=None):
        self.uploads.append((str(local_path), remote_path, mode))

    def download(self, remote_path, local_path):
        self.downloads.append((remote_path, str(local_path)))

    def upload_dir(self, local_dir, remote_dir, *, excludes=(), delete=False):
        self.uploads.append((str(local_dir), remote_dir, "dir"))

    def write_file(self, content, remote_path, *, mode=None):
        self.files[remote_path] = (content, mode)

    def exists(self, remote_path):
        return remote_path in self.files

    def mkdirs(self, remote_path, *, mode=None):
        self.dirs.append(remote_path)

    def home(self):
        return self._home

    def expand(self, path):
        text = str(path)
        if text.startswith("~/"):
            return posixpath.join(self._home, text[2:])
        return text
