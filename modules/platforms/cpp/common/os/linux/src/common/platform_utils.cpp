/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#include <cstdio>
#include <ctime>

#include <sys/stat.h>
#include <sys/time.h>
#include <dirent.h>
#include <dlfcn.h>
#include <glob.h>
#include <unistd.h>
#include <ftw.h>

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

        static int rmFiles(const char *pathname, const struct stat *sbuf, int type, struct FTW *ftwb)
        {
            remove(pathname);

            return 0;
        }

        bool DeletePath(const std::string& path)
        {
            return nftw(path.c_str(), rmFiles, 10, FTW_DEPTH | FTW_MOUNT | FTW_PHYS) == 0;
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
