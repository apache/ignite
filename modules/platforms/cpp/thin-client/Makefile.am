#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
# 
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
ACLOCAL_AMFLAGS =-I m4

lib_LTLIBRARIES = libignite-thin-client.la

SUBDIRS = \
    include

AM_CPPFLAGS = \
    -I$(srcdir)/include \
    -I$(srcdir)/src \
    -I@top_srcdir@/common/include \
    -I@top_srcdir@/common/os/linux/include \
    -I@top_srcdir@/binary/include \
    -I@top_srcdir@/network/include \
    -I@top_srcdir@/network/os/linux/include \
    -DIGNITE_IMPL \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
    -Wall \
    -std=c++03

libignite_thin_client_la_LIBADD = \
    @top_srcdir@/binary/libignite-binary.la \
    @top_srcdir@/network/libignite-network.la

libignite_thin_client_la_LDFLAGS = \
    -no-undefined \
    -ldl \
    -version-info 0:0:0 \
    -release $(PACKAGE_VERSION)

libignite_thin_client_la_DEPENDENCIES = \
    @top_srcdir@/binary/libignite-binary.la \
    @top_srcdir@/network/libignite-network.la

libignite_thin_client_la_SOURCES = \
    src/impl/utility.cpp \
    src/impl/protocol_version.cpp \
    src/impl/data_channel.cpp \
    src/impl/remote_type_updater.cpp \
    src/impl/message.cpp \
    src/impl/ignite_client_impl.cpp \
    src/impl/cache/cache_client_proxy.cpp \
    src/impl/cache/cache_client_impl.cpp \
    src/impl/cache/cache_affinity_info.cpp \
    src/impl/data_router.cpp \
    src/ignite_client.cpp

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)
