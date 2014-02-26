// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifdef _MSC_VER

#define WIN32_LEAN_AND_MEAN // Solves Boost include problems on Windows.

#include <memory>
#include <unordered_map>
#include <cstdint>
#include <cstring>

#define strtoll _strtoi64

#else

#include <memory>
#include <unordered_map>
#include <cstring>

#endif
