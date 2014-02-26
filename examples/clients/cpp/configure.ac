dnl Process this file with autoconf to produce a configure script.

AC_PREREQ(2.68)
AC_INIT([GridGain C++ client examples],[4.1.0],[info@gridgain.com],[gridgain-client-examples],[www.gridgain.com])
AC_CONFIG_SRCDIR([src])
AC_CONFIG_MACRO_DIR([m4])
AC_LANG([C++])

AC_CANONICAL_SYSTEM

dnl Initialize automake
AM_INIT_AUTOMAKE([-Wall foreign])
AC_CONFIG_HEADER([config.h])

GXX="-g -O2"

AC_PROG_CXX

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