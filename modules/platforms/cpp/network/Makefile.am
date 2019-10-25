##
## Licensed to the Apache Software Foundation (ASF) under one or more
## contributor license agreements.  See the NOTICE file distributed with
## this work for additional information regarding copyright ownership.
## The ASF licenses this file to You under the Apache License, Version 2.0
## (the "License"); you may not use this file except in compliance with
## the License.  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##

ACLOCAL_AMFLAGS =-I m4

lib_LTLIBRARIES = libignite-network.la

SUBDIRS = \
	include

AM_CPPFLAGS = \
	-I$(srcdir)/include \
	-I$(srcdir)/src \
	-I$(srcdir)/os/linux/include \
	-I$(srcdir)/os/linux/src \
	-I@top_srcdir@/common/include \
	-I@top_srcdir@/common/os/linux/include \
	-DIGNITE_IMPL \
	-D__STDC_LIMIT_MACROS \
	-D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
	-Wall \
	-std=c++03

libignite_network_la_LIBADD = \
	@top_srcdir@/common/libignite-common.la

libignite_network_la_LDFLAGS = \
	-no-undefined \
	-version-info 0:0:0 \
	-release $(PACKAGE_VERSION)

libignite_network_la_DEPENDENCIES = \
	@top_srcdir@/common/libignite-common.la

libignite_network_la_SOURCES = \
	os/linux/src/network/utils.cpp \
	os/linux/src/network/tcp_socket_client.cpp \
	os/linux/src/network/sockets.cpp \
	src/network/network.cpp \
	src/network/ssl/secure_socket_client.cpp \
	src/network/ssl/ssl_gateway.cpp

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)
