##
##                   GridGain Community Edition Licensing
##                   Copyright 2019 GridGain Systems, Inc.
##
## Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
## Restriction;  you may not use this file except in compliance with the License. You may obtain a
## copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software distributed under the
## License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied. See the License for the specific language governing permissions
## and limitations under the License.
##
## Commons Clause Restriction
##
## The Software is provided to you by the Licensor under the License, as defined below, subject to
## the following condition.
##
## Without limiting other conditions in the License, the grant of rights under the License will not
## include, and the License does not grant to you, the right to Sell the Software.
## For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
## under the License to provide to third parties, for a fee or other consideration (including without
## limitation fees for hosting or consulting/ support services related to the Software), a product or
## service whose value derives, entirely or substantially, from the functionality of the Software.
## Any license notice or attribution required by the License must also include this Commons Clause
## License Condition notice.
##
## For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
## the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
## Edition software provided with this notice.
##

ACLOCAL_AMFLAGS =-I m4

SUBDIRS = \
    include

lib_LTLIBRARIES = libignite-jni.la

AM_CPPFLAGS = \
    -I$(srcdir)/include \
    -I$(srcdir)/os/linux/include \
    -I@top_srcdir@/common/include \
    -I@top_srcdir@/common/os/linux/include \
    -I$(JAVA_HOME)/include \
    -I$(JAVA_HOME)/include/linux \
    -DIGNITE_IMPL \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
    -Wall \
    -std=c++03

libignite_jni_la_LIBADD = \
    -L$(JAVA_HOME)/jre/lib/amd64/server \
    -L$(JAVA_HOME)/lib/server \
    @top_srcdir@/common/libignite-common.la

libignite_jni_la_LDFLAGS = \
    -no-undefined \
    -ljvm \
    -version-info 0:0:0 \
    -release $(PACKAGE_VERSION)

libignite_jni_la_DEPENDENCIES = \
    @top_srcdir@/common/libignite-common.la

libignite_jni_la_SOURCES = \
    os/linux/src/utils.cpp \
    src/java.cpp \
    src/exports.cpp

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)


