// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTLOG_HPP_INCLUDED
#define GRIDCLIENTLOG_HPP_INCLUDED

#include <string>

#define GG_LOG_ERROR(m, ...) if(GridClientLog::isLevel(GridClientLog::LEVEL_ERROR)) \
    GridClientLog::log(GridClientLog::LEVEL_ERROR,__FILE__,__LINE__,__FUNCTION__, m, __VA_ARGS__)

#define GG_LOG_AND_THROW(ex, m, ...) {\
    if(GridClientLog::isLevel(GridClientLog::LEVEL_ERROR))\
        GG_LOG_ERROR(m, __VA_ARGS__);\
\
    throw ex(fmtstring(m, __VA_ARGS__));\
}

#define GG_LOG_ERROR0(m) GG_LOG_ERROR(m, NULL)

#define GG_LOG_WARN(m, ...) if(GridClientLog::isLevel(GridClientLog::LEVEL_WARN)) \
    GridClientLog::log(GridClientLog::LEVEL_WARN,__FILE__,__LINE__,__FUNCTION__, m, __VA_ARGS__)

#define GG_LOG_WARN0(m) GG_LOG_WARN(m, NULL)

#define GG_LOG_INFO(m, ...) if(GridClientLog::isLevel(GridClientLog::LEVEL_INFO)) \
    GridClientLog::log(GridClientLog::LEVEL_INFO,__FILE__,__LINE__,__FUNCTION__, m, __VA_ARGS__)

#define GG_LOG_INFO0(m) GG_LOG_INFO(m, NULL)

#define GG_LOG_DEBUG(m, ...) if(GridClientLog::isLevel(GridClientLog::LEVEL_DEBUG)) \
    GridClientLog::log(GridClientLog::LEVEL_DEBUG,__FILE__,__LINE__,__FUNCTION__,m, __VA_ARGS__)

#define GG_LOG_DEBUG0(m) GG_LOG_DEBUG(m, NULL)

std::string fmtstring(const char* fmt, ...);

struct GridClientLog {
public:
    enum Level {
        LEVEL_ERROR = 1, LEVEL_WARN, LEVEL_INFO, LEVEL_DEBUG
    };

    static void log(Level level, const char* file, int line, const char* funcName, const char* format, ...);

    static bool isLevel(Level level);
};

#endif /* GRIDCLIENTLOG_HPP_INCLUDED */
