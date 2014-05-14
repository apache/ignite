dnl Process this file with autoconf to produce a configure script.

AC_PREREQ(2.68)
AC_INIT([GridGain C++ client],[6.1.5],[info@gridgain.com],[gridgain-client],[www.gridgain.com])
AC_CONFIG_SRCDIR([gridtestcommon.hpp])

AC_CANONICAL_SYSTEM
AC_CONFIG_MACRO_DIR([m4])
AC_LANG([C++])

dnl Initialize automake
AM_INIT_AUTOMAKE([-Wall foreign])
AC_CONFIG_HEADER(config.h)

GXX="-g -O2"
if test "$target_os" == cygwin; then
    CPPFLAGS="-D_WIN32_WINNT=0x0501 -D__USE_W32_SOCKETS"
    LDFLAGS=-lws2_32
fi

AC_PROG_CXX

dnl Initialize Libtool
LT_INIT

dnl Check if Libtool is present
dnl Libtool is used for building share libraries
AC_PROG_LIBTOOL

dnl Debug option
AC_ARG_ENABLE([debug],
 [AS_HELP_STRING([--enable-debug],[enable debug build [default=no]])],
 [],[enable_debug=no])

dnl Coverage option
AC_ARG_ENABLE([coverage],
 [AS_HELP_STRING([--enable-coverage],[enable code coverage [default=no]])],
 [],[enable_coverage=no])

if test "x$enable_debug" = xyes; then
    CXXFLAGS="-g -O0"
elif test "x$enable_coverage" = xyes; then
    CXXFLAGS="-g -O0 --coverage"
else
    CXXFLAGS="-O3"
fi

dnl Profiling option
AC_ARG_ENABLE([profiling],
 [AS_HELP_STRING([--enable-profiling],[enable profiling [default=no]])],
 [],[enable_profiling=no])

if test "x$enable_profiling" = xyes; then
    CXXFLAGS="$CXXFLAGS -pg"
fi


dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE
AC_TYPE_INT16_T
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_INT8_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T

AC_FUNC_ERROR_AT_LINE
AC_CHECK_FUNCS([gettimeofday localtime_r memset socket])

AC_CONFIG_FILES([Makefile])

AC_OUTPUT
