// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCONF_HPP_INCLUDED
#define GRIDCONF_HPP_INCLUDED

#if defined(__CYGWIN__) || defined(WIN32)
# ifdef GRIDGAIN_EXPORT
#    define GRIDGAIN_API __declspec(dllexport)
#    define EXPIMP_TEMPLATE
# else
#    define GRIDGAIN_API __declspec(dllimport)
#    define EXPIMP_TEMPLATE extern
# endif
#else
# define GRIDGAIN_API
#endif

#endif
