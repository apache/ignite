# Include macros from m4 folder.
ACLOCAL_AMFLAGS = "-Im4"

#######################################
# The list of libraries we are building seperated by spaces.
# The 'lib_' indicates that these build products will be installed
# in the $(libdir) directory. For example /usr/lib
lib_LTLIBRARIES = libggshmem.la

#######################################
# Build information for each library

# Sources for ggshmem
libggshmem_la_SOURCES = org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils.cpp

# Linker options libTestProgram
libggshmem_la_LDFLAGS =

# Compiler options. Here we are adding the include directory
# to be searched for headers included in the source code.
libggshmem_la_CPPFLAGS = -I$(top_srcdir)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(JAVA_HOME)/include/darwin \
-I/System/Library/Frameworks/JavaVM.framework/Headers

