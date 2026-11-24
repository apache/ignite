#!/usr/bin/env python3
#
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
#

"""
Prints the public C++ API of the Ignite source tree in the current directory, one declaration per line: area, key, value,
flags and location, separated by tabs. The public API is what the headers that the modules install declare, except the impl
directories and namespaces, which Ignite uses for internal code, and the service namespaces in SERVICE. Clang parses all those
headers in one translation unit, as users include them.
"""

import json
import os
import subprocess
import sys
import tempfile

CPP = 'modules/platforms/cpp'

# Modules whose CMake builds install their headers for users.
PUBLIC = ['binary', 'common', 'core', 'network', 'thin-client']

# Namespaces of ignite that hold service code. The examples do not use them, jni and network appear in no public signature,
# and the types of common that public signatures use are compared through those signatures. Ignite does not define its C++
# API: cpp.dxg, which builds the C++ documentation, points at directories that do not exist.
SERVICE = {'common', 'jni', 'network'}

AREA = 'C++ API'
FUNCTIONS = {'FunctionDecl', 'CXXMethodDecl', 'CXXConstructorDecl', 'CXXDestructorDecl', 'CXXConversionDecl'}


def headers():
    """Returns the public headers as users include them, such as ignite/thin/ignite_client.h."""
    res = []

    for module in PUBLIC:
        base = os.path.join(CPP, module, 'include')

        for d, _, files in os.walk(os.path.join(base, 'ignite')):
            for f in files:
                path = os.path.relpath(os.path.join(d, f), base)

                if f.endswith('.h') and '/impl/' not in '/' + path:
                    res.append(path)

    return sorted(res)


def public_file(path):
    """Returns True if declarations in the file are public: a public header outside the impl directories."""
    return any(path.startswith(m + '/include/ignite/') for m in PUBLIC) and '/impl/' not in path


def ast():
    """Returns the top-level ignite namespaces that clang parsed."""
    java = os.environ['JAVA_HOME']
    os_dir = 'linux' if sys.platform.startswith('linux') else 'darwin'
    includes = ['-I' + m + '/include' for m in PUBLIC] + ['-Icommon/os/linux/include', '-I' + java + '/include',
                                                           '-I' + os.path.join(java, 'include', os_dir)]

    with tempfile.NamedTemporaryFile('w', suffix='.cpp', delete=False) as unit:
        unit.write('#include <cassert>\n' + ''.join('#include <%s>\n' % h for h in headers()))

    try:
        out = subprocess.run(['clang', '-x', 'c++', '-std=c++11', '-fsyntax-only', *includes, '-Xclang', '-ast-dump=json',
                              '-Xclang', '-ast-dump-filter=ignite', unit.name], cwd=CPP, check=True, capture_output=True,
                             text=True).stdout
    finally:
        os.unlink(unit.name)

    decoder = json.JSONDecoder()
    pos = 0
    res = []

    while True:
        while pos < len(out) and out[pos].isspace():
            pos += 1

        if pos == len(out):
            return res

        obj, pos = decoder.raw_decode(out, pos)

        if obj.get('kind') == 'NamespaceDecl' and obj.get('name') == 'ignite':
            res.append(obj)


class Walker:
    """Walks one dumped namespace. Clang leaves out the file and line of a location that repeats the previous one."""

    def __init__(self, out):
        self.out = out
        self.file = None
        self.line = None

    def track(self, loc):
        """Updates the current position and returns the position of a location, taking the macro expansion of a macro."""
        if not loc:
            return self.file, self.line

        if 'spellingLoc' in loc:
            self.track(loc['spellingLoc'])

            return self.track(loc['expansionLoc'])

        self.file = loc.get('file', self.file)
        self.line = loc.get('line', self.line)

        return self.file, self.line

    def position(self, node):
        """Returns the position of a node and moves past its range."""
        at = self.track(node.get('loc'))
        rng = node.get('range') or {}

        self.track(rng.get('begin'))
        self.track(rng.get('end'))

        return at

    def walk(self, node, scope, record):
        """Describes a declaration and the declarations inside it."""
        at = self.position(node)
        kind = node.get('kind')
        name = node.get('name', '')
        inner = node.get('inner', [])

        if node.get('isImplicit') or (record and node.get('access') not in (None, 'public', 'protected')):
            self.skip(inner)

            return

        deprecated = any(c.get('kind') == 'DeprecatedAttr' for c in inner)

        if kind == 'NamespaceDecl':
            if name in ('impl', 'detail') or (scope == ['ignite'] and name in SERVICE):
                self.skip(inner)
            else:
                self.children(inner, scope + [name], False)

            return

        if kind in ('ClassTemplateDecl', 'FunctionTemplateDecl'):
            params = [c.get('name', '') for c in inner if c.get('kind', '').endswith('ParmDecl')]

            for c in inner:
                if kind == 'ClassTemplateDecl' and c.get('kind') == 'CXXRecordDecl':
                    self.record(c, scope, self.position(c), params, deprecated)
                elif kind == 'FunctionTemplateDecl' and c.get('kind') in FUNCTIONS:
                    self.function(c, scope, self.position(c), params, deprecated)
                    self.skip(c.get('inner', []))
                else:
                    self.position(c)
                    self.skip(c.get('inner', []))

            return

        if kind == 'CXXRecordDecl':
            self.record(node, scope, at, [], deprecated)

            return

        if kind in FUNCTIONS:
            self.function(node, scope, at, None, deprecated)
        elif kind in ('FieldDecl', 'VarDecl', 'TypedefDecl', 'TypeAliasDecl'):
            value = (node.get('storageClass', '') + ' ' if kind == 'VarDecl' else '') + \
                ('typedef ' if kind != 'FieldDecl' and kind != 'VarDecl' else '') + node.get('type', {}).get('qualType', '')
            self.emit(scope + [name], value.strip(), deprecated, at)
        elif kind == 'EnumDecl':
            self.enum(node, scope, at, deprecated)

            return

        self.skip(inner)

    def record(self, node, scope, at, params, deprecated):
        """Describes a class, struct or union with its public and protected members."""
        inner = node.get('inner', [])
        name = node.get('name', '')
        deprecated = deprecated or any(c.get('kind') == 'DeprecatedAttr' for c in inner)

        if not node.get('completeDefinition') or not name:
            self.skip(inner)

            return

        bases = ', '.join(b.get('access', '') + ' ' + b['type']['qualType'] for b in node.get('bases', []))
        tmpl = '<' + ', '.join(params) + '> ' if params else ''

        self.emit(scope + [name], ('template ' + tmpl if params else '') + node.get('tagUsed', 'class') + ' ' + name +
                  (' : ' + bases if bases else ''), deprecated, at)
        self.children(inner, scope + [name], True)

    def function(self, node, scope, at, tparams, deprecated):
        """
        Describes a function or method by its name, template parameters, parameter types and qualifiers such as const.

        :param tparams: Template parameters of a function template, or None.
        """
        params = [c.get('type', {}).get('qualType', '') for c in node.get('inner', []) if c.get('kind') == 'ParmVarDecl']
        signature = node.get('type', {}).get('qualType', '')
        qualifiers = signature[signature.rfind(')') + 1:]
        spec = ''.join(s + ' ' for s in ('virtual', 'static', 'inline') if node.get(s)) + \
            ('= 0 ' if node.get('pure') else '') + ('= delete ' if node.get('explicitlyDeleted') else '')

        tmpl = '<' + ', '.join(tparams) + '>' if tparams is not None else ''

        self.emit(scope + [node.get('name', '') + tmpl + '(' + ', '.join(params) + ')' + qualifiers],
                  (('template ' if tparams is not None else '') + spec + signature).strip(), deprecated, at)

    def enum(self, node, scope, at, deprecated):
        """
        Describes an enum and its constants with their positions and explicit values. The constants of an unnamed enum belong to
        the enclosing scope.
        """
        name = node.get('name', '')
        inner = node.get('inner', [])
        fixed = node.get('fixedUnderlyingType', {}).get('qualType')
        names = scope + [name] if name else scope

        if name:
            self.emit(names, 'enum' + (' class' if node.get('scopedEnumTag') else '') + (' : ' + fixed if fixed else ''), deprecated,
                      at)

        idx = 0

        for c in inner:
            c_at = self.position(c)

            if c.get('kind') != 'EnumConstantDecl':
                self.skip(c.get('inner', []))

                continue

            value = next((e.get('value') for e in c.get('inner', []) if e.get('kind') == 'ConstantExpr'), None)

            self.skip(c.get('inner', []))
            self.emit(names + [c.get('name', '')], '#%d' % idx + (' = ' + value if value is not None else ''), False, c_at)

            idx += 1

    def children(self, inner, scope, record):
        """Describes the declarations inside a namespace or record."""
        for c in inner:
            self.walk(c, scope, record)

    def skip(self, inner):
        """Moves past nodes that are not described, keeping the position that clang leaves out of later nodes."""
        for c in inner:
            self.position(c)
            self.skip(c.get('inner', []))

    def emit(self, names, value, deprecated, at):
        """Prints a declaration that a public header declares."""
        file, line = at

        if not file or not public_file(file):
            return

        key = '::'.join(names)

        for text in (key, value):
            if '\t' in text or '\n' in text:
                raise ValueError('Tab or line break in ' + repr(text))

        self.out.write('\t'.join([AREA, key, ('[[deprecated]] ' if deprecated else '') + value,
                                  'deprecated' if deprecated else '', '%s/%s:%s' % (CPP, file, line)]) + '\n')


def main():
    namespaces = ast()

    if not namespaces:
        sys.exit('Clang found no ignite namespace in the public headers')

    for ns in namespaces:
        Walker(sys.stdout).walk(ns, [], False)


if __name__ == '__main__':
    main()
