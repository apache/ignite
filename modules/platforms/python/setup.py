# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import defaultdict
import setuptools


def is_a_requirement(line: str) -> bool:
    return not any([
        line.startswith('#'),
        line.startswith('-r'),
        len(line) == 0,
    ])


requirement_sections = [
    'install',
    'setup',
    'tests',
    'docs',
]
requirements = defaultdict(list)

for section in requirement_sections:
    with open('requirements/{}.txt'.format(section), 'r') as requirements_file:
        for line in requirements_file.readlines():
            line = line.strip('\n')
            if is_a_requirement(line):
                requirements[section].append(line)

with open('README.md', 'r') as readme_file:
    long_description = readme_file.read()

setuptools.setup(
    name='pyignite',
    version='0.1.7',
    author='Dmitry Melnichuk',
    author_email='dmitry.melnichuk@nobitlost.com',
    description='Apache Ignite binary client Python API',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url=(
        'https://github.com/nobitlost'
        '/ignite/tree/ignite-7782/modules/platforms/python'
    ),
    packages=setuptools.find_packages(),
    install_requires=requirements['install'],
    tests_require=requirements['tests'],
    setup_requires=requirements['setup'],
    extras_require={
        'docs': requirements['docs'],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Intended Audience :: Developers',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
)
