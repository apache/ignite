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


import hashlib
import os

from ducktape.tests.test import TestContext


def decorate_args(args, with_args=False):
    """
    Decorate args with sha1 hash.
    """
    prefix = ''
    if args:
        sha_1 = hashlib.sha1()
        sha_1.update(args.encode('utf-8'))

        digest = sha_1.hexdigest()[0:6]
        prefix = digest + '@'

    return prefix + args if with_args else prefix


def patched_test_name(self):
    """
    Monkey patched test_name property function.
    """
    name_components = [self.module_name,
                       self.cls_name,
                       self.function_name,
                       self.injected_args_name]

    name = ".".join(filter(lambda x: x is not None and len(x) > 0, name_components))
    return decorate_args(self.injected_args_name) + name


def patched_results_dir(test_context, test_index):
    """
    Monkey patch results_dir.
    """
    results_dir = test_context.session_context.results_dir

    if test_context.cls is not None:
        results_dir = os.path.join(results_dir, test_context.cls.__name__)
    if test_context.function is not None:
        results_dir = os.path.join(results_dir, test_context.function.__name__)
    if test_context.injected_args is not None:
        results_dir = os.path.join(results_dir, decorate_args(test_context.injected_args_name, True))
    if test_index is not None:
        results_dir = os.path.join(results_dir, str(test_index))

    return results_dir


TestContext.test_name = property(patched_test_name)
TestContext.results_dir = staticmethod(patched_results_dir)
