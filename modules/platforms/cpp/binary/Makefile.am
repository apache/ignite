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

ACLOCAL_AMFLAGS = "-Im4"

SUBDIRS = . include
DIST_SUBDIRS = . include

AM_CPPFLAGS = -I$(srcdir)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -DIGNITE_IMPL
AM_CXXFLAGS = -Wall -std=c++0x
LIB_LDFLAGS = -no-undefined -version-info 1

COMMON_SRC = src/binary/binary_containers.cpp \
             src/binary/binary_raw_writer.cpp \
             src/binary/binary_writer.cpp \
             src/binary/binary_reader.cpp \
             src/binary/binary_type.cpp \
             src/binary/binary_raw_reader.cpp \
             src/impl/ignite_environment.cpp \
             src/impl/binary/binary_type_manager.cpp \
             src/impl/binary/binary_utils.cpp \
             src/impl/binary/binary_type_updater_impl.cpp \
             src/impl/binary/binary_reader_impl.cpp \
             src/impl/binary/binary_type_handler.cpp \
             src/impl/binary/binary_writer_impl.cpp \
             src/impl/binary/binary_type_updater.cpp \
             src/impl/binary/binary_schema.cpp \
             src/impl/binary/binary_type_snapshot.cpp \
             src/impl/interop/interop_memory.cpp \
             src/impl/interop/interop_output_stream.cpp \
             src/impl/interop/interop_input_stream.cpp \
             src/guid.cpp \
             src/date.cpp \
             src/timestamp.cpp

lib_LTLIBRARIES = libignite-binary.la
libignite_binary_la_SOURCES = $(COMMON_SRC)
libignite_binary_la_LIBADD = -L$(JAVA_HOME)/jre/lib/amd64/server
libignite_binary_la_LDFLAGS = $(LIB_LDFLAGS) -L/usr/local/lib -lignite-common -ljvm -version-info 0:0:0 -release $(PACKAGE_VERSION)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = ignite-binary.pc

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)
