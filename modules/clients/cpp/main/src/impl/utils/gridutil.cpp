// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <cstdio>
#include <cstdlib>
#include <ctime>

#include <boost/lexical_cast.hpp>

#ifdef _MSC_VER // Windows

#include <windows.h>

#include "resource.h"

#define VER_STR /*@cpp.version*/GG_VERSION_STR_WIN

#else

#include <sys/time.h>

#include "config.h"

#define VER_STR /*@cpp.version*/PACKAGE_VERSION

#endif

#include "gridgain/gridclientdatametrics.hpp"
#include "gridgain/gridclientnode.hpp"
#include "gridgain/impl/utils/gridutil.hpp"
#include "gridgain/gridclientvariant.hpp"
#include "gridgain/impl/utils/gridclientlog.hpp"
#include "gridgain/impl/hash/gridclientstringhasheableobject.hpp"
#include "gridgain/impl/hash/gridclientsimpletypehasheableobject.hpp"

std::string GridUtil::prependHome(std::string relPath) {
    char *ggHome;
    ggHome = getenv("GRIDGAIN_HOME");

    if (ggHome == NULL) {
        GG_LOG_ERROR0("Please configure GRIDGAIN_HOME environment variable.");
        return relPath;
    }

    std::string ggStr = ggHome;

    ggStr += "/";
    ggStr += relPath;

    GG_LOG_DEBUG("Prepend home returns file: %s", ggStr.c_str());

    return ggStr;
}

std::ostream& GridUtil::toStream(std::ostream &out, const std::vector<std::string> &v) {
    for (size_t i = 0; i < v.size(); ++i) {
        if (i != 0)
            out << ",";

        out << v[i];
    }
    return out;
}

std::ostream& GridUtil::toStream(std::ostream &out, const std::vector<GridSocketAddress> &v) {
    for (size_t i = 1; i < v.size(); ++i) {
        if (i != 0)
            out << ",";

        out << v[i].host() << ":" << v[i].port();
    }
    return out;
}

int64_t GridUtil::currentTimeMillis() {
#ifdef _MSC_VER // Windows
    /**
     * Contains a 64-bit value representing the number of 100-nanosecond intervals since
     * January 1, 1601 (UTC).
     *
     * http://msdn.microsoft.com/en-us/library/windows/desktop/ms724284(v=vs.85).aspx
     */
    FILETIME ft;

    GetSystemTimeAsFileTime(&ft);

    int64_t ret = (int64_t(ft.dwHighDateTime) << 32) | int64_t(ft.dwLowDateTime);

    return ret / 10000; //we use milliseconds
#else
    struct timeval tv;
    ::gettimeofday(&tv, NULL);

    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
#endif
}

std::vector<int8_t> GridUtil::getVersionNumeric() {
    std::vector<int8_t> ret(4, 0); // Vector of 4 zeros.

    GridClientByteUtils::valueToBytes(GridStringHasheableObject(VER_STR).hashCode(), &ret[0], ret.size(),
                    GridClientByteUtils::BIG_ENDIAN_ORDER);

    return ret;
}
