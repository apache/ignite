/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <time.h>

#include <sys/stat.h>
#include <sys/time.h>
#include <dirent.h>
#include <dlfcn.h>
#include <glob.h>
#include <unistd.h>

#include <ignite/common/utils.h>

namespace ignite
{
    namespace common
    {
        time_t IgniteTimeGm(const tm& time)
        {
            tm tmc = time;

            return timegm(&tmc);
        }

        time_t IgniteTimeLocal(const tm& time)
        {
            tm tmc = time;

            return mktime(&tmc);
        }

        bool IgniteGmTime(time_t in, tm& out)
        {
            return gmtime_r(&in, &out) != NULL;
        }

        bool IgniteLocalTime(time_t in, tm& out)
        {
            return localtime_r(&in, &out) == 0;
        }

        std::string GetEnv(const std::string& name)
        {
            static const std::string empty;

            return GetEnv(name, empty);
        }

        std::string GetEnv(const std::string& name, const std::string& dflt)
        {
            char* val0 = std::getenv(name.c_str());

            if (!val0)
                return dflt;

            return std::string(val0);
        }

        bool FileExists(const std::string& path)
        {
            glob_t gs;

            int res = glob(path.c_str(), 0, 0, &gs);

            globfree(&gs);

            return res == 0;
        }

        bool IsValidDirectory(const std::string& path)
        {
            if (path.empty())
                return false;

            struct stat pathStat;

            return stat(path.c_str(), &pathStat) != -1 && S_ISDIR(pathStat.st_mode);
        }

        StdCharOutStream& Fs(StdCharOutStream& ostr)
        {
            ostr.put('/');
            return ostr;
        }

        StdCharOutStream& Dle(StdCharOutStream& ostr)
        {
            static const char expansion[] = ".so";

            ostr.write(expansion, sizeof(expansion) - 1);

            return ostr;
        }

        unsigned GetRandSeed()
        {
            timespec ts;

            clock_gettime(CLOCK_MONOTONIC, &ts);

            unsigned res = static_cast<unsigned>(ts.tv_sec);
            res ^= static_cast<unsigned>(ts.tv_nsec);
            res ^= static_cast<unsigned>(getpid());

            return res;
        }
    }
}
