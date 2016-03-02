#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([Ingnite C++ examples],[1.6.0.8653],[dec@ignite.apache.org],[ignite-examples],[ignite.apache.org])
AC_CONFIG_SRCDIR(src)

AC_CANONICAL_SYSTEM
AC_CONFIG_MACRO_DIR([m4])
AC_LANG([C++])

# Initialize automake
AM_INIT_AUTOMAKE([-Wall foreign subdir-objects])
AC_CONFIG_HEADER(config.h)

AM_PROG_AR

# Checks for programs.
GXX="-g -O2"

AC_PROG_CXX

# Initialize Libtool
LT_INIT

AC_ARG_ENABLE([debug],
 [AS_HELP_STRING([--enable-debug],[enable debug build [default=no]])],
 [],[enable_debug=no])

if test "x$enable_debug" = xyes; then
    CXXFLAGS="-g -O0"
else
    CXXFLAGS="-g -O3"
fi

AC_CONFIG_FILES(Makefile)

AC_OUTPUT
