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

noinst_PROGRAMS = ignite-queryexample

AM_CPPFLAGS = \
    -I@top_srcdir@/include \
    -I@top_srcdir@/../core/include \
    -I@top_srcdir@/../core/os/linux/include \
    -I@top_srcdir@/../common/include \
    -I@top_srcdir@/../common/os/linux/include \
    -I@top_srcdir@/../binary/include \
    -I@top_srcdir@/../jni/include \
    -I@top_srcdir@/../jni/os/linux/include \
    -I$(JAVA_HOME)/include \
    -I$(JAVA_HOME)/include/linux \
    -DIGNITE_IMPL \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
    -Wall \
    -std=c++03

ignite_queryexample_LDADD = \
    @top_srcdir@/../core/libignite.la \
    -lpthread

ignite_queryexample_LDFLAGS = \
    -static-libtool-libs

ignite_queryexample_SOURCES = \
    src/query_example.cpp

run-check: check
	./ignite-queryexample -p

clean-local: clean-check
	$(RM) *.gcno *.gcda

clean-check:
	$(RM) $(ignite_queryexample_OBJECTS)
