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
# Include macros from m4 folder.
ACLOCAL_AMFLAGS = "-Im4"

#######################################
# The list of libraries we are building seperated by spaces.
# The 'lib_' indicates that these build products will be installed
# in the $(libdir) directory. For example /usr/lib
lib_LTLIBRARIES = libigniteshmem.la

#######################################
# Build information for each library

# Sources for igniteshmem
libigniteshmem_la_SOURCES = org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils.cpp

# Linker options libTestProgram
libigniteshmem_la_LDFLAGS =

# Compiler options. Here we are adding the include directory
# to be searched for headers included in the source code.
libigniteshmem_la_CPPFLAGS = -I$(top_srcdir)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(JAVA_HOME)/include/darwin \
-I/System/Library/Frameworks/JavaVM.framework/Headers

