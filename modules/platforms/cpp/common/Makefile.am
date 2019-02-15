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

lib_LTLIBRARIES = libignite-common.la

SUBDIRS = \
	include \
	os/linux/include

AM_CPPFLAGS = \
	-I$(srcdir)/include \
	-I$(srcdir)/os/linux/include \
	-DIGNITE_IMPL \
	-D__STDC_LIMIT_MACROS \
	-D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
	-Wall \
	-std=c++03

libignite_common_la_LDFLAGS = \
	-no-undefined \
	-L/usr/local/lib \
	-ldl \
	-version-info 0:0:0 \
	-release $(PACKAGE_VERSION)

libignite_common_la_SOURCES = \
	os/linux/src/common/concurrent_os.cpp \
	os/linux/src/common/platform_utils.cpp \
	os/linux/src/common/dynamic_load_os.cpp \
	src/common/big_integer.cpp \
	src/common/bits.cpp \
	src/common/concurrent.cpp \
	src/common/decimal.cpp \
	src/common/utils.cpp \
	src/date.cpp \
	src/ignite_error.cpp \
	src/guid.cpp \
	src/timestamp.cpp \
	src/time.cpp

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)
