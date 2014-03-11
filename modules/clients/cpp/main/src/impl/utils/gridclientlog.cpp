/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <cstdio>
#include <cstdarg>
#include <cstdlib>
#include <iostream>

#ifndef _MSC_VER 
#include <sys/time.h>
#else
#include <process.h>
#define pid_t int
#include <sys/timeb.h>
#include <ctime>
#endif

#include <boost/thread.hpp>

#include "gridgain/impl/utils/gridclientlog.hpp"

using namespace std;

#define TIME_NOW_BUF_SIZE 1024
#define FORMAT_LOG_BUF_SIZE 4096

#ifndef _MSC_VER
static const char* time_now(char* now_str) {
    timeval tv;
    tm lt;

    gettimeofday(&tv, 0);

    time_t now = tv.tv_sec;
    localtime_r(&now, &lt);

    // Clone the format used by log4j ISO8601DateFormat,
    // specifically: "yyyy-MM-dd HH:mm:ss.SSS"
    size_t len = strftime(now_str, TIME_NOW_BUF_SIZE, "%Y-%m-%d %H:%M:%S", &lt);

    len += snprintf(now_str + len, TIME_NOW_BUF_SIZE - len, ".%03d", (int) (tv.tv_usec / 1000));

    return now_str;
}

#else
static const char* time_now(char* now_str) {
    struct _timeb tb;
    _ftime_s(&tb);

    // Convert time to struct tm form 
    tm* lt = localtime(&tb.time);

    size_t len=strftime(now_str, TIME_NOW_BUF_SIZE, "%Y-%m-%d %H:%M:%S", lt);

    len += _snprintf_s(now_str + len, TIME_NOW_BUF_SIZE - len, 4,".%03d", tb.millitm);

    return now_str;
}
#endif

std::string fmtstring(const char* fmt, ...) {
    va_list va;
    va_start(va, fmt);

    const size_t SIZE = 512;
    char buffer[SIZE] = { 0 };
    vsnprintf(buffer, SIZE, fmt, va);

    va_end(va);

    return std::string(buffer);
}

static struct Log {
    Log(): level(GridClientLog::LEVEL_INFO) {
        char* lvl = getenv("GRIDGAIN_CPP_CLIENT_LOG_LEVEL");

        if (lvl != NULL) {
            int i = atoi(lvl);

            if (i < GridClientLog::LEVEL_ERROR)
                level = GridClientLog::LEVEL_ERROR;
            else if (i > GridClientLog::LEVEL_DEBUG)
                level = GridClientLog::LEVEL_DEBUG;
            else
                level = (GridClientLog::Level) i;
        }
    }

    GridClientLog::Level level;
} logInstance;

bool GridClientLog::isLevel(GridClientLog::Level level) {
    return logInstance.level >= level;
}

void GridClientLog::log(GridClientLog::Level level, const char* file, int line, const char* funcName,
        const char* format, ...) {
    static const char* dbgLevelStr[] = { "INVALID", "ERROR", "WARN", "INFO", "DEBUG" };
    static pid_t pid = getpid();

    char timebuf[TIME_NOW_BUF_SIZE];
    char msgbuf[FORMAT_LOG_BUF_SIZE];
    va_list va;

    va_start(va, format);
    vsnprintf(msgbuf, FORMAT_LOG_BUF_SIZE - 1, format, va);
    va_end(va);

    ostringstream os;

    os << time_now(timebuf) << " pid:" << pid << "(tid:" << boost::this_thread::get_id() << ") "
        << dbgLevelStr[level];

    if (level == GridClientLog::LEVEL_ERROR || level == GridClientLog::LEVEL_DEBUG)
        os << " " << file << ":" << funcName << ":" << line << ":";

    os << " " << msgbuf << endl;

    cout << os.str() << flush;
}
