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
#include <dirent.h>
#include <dlfcn.h>

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

        std::string GetEnv(const std::string& name, bool& found)
        {
            char* val = std::getenv(name.c_str());

            if (val)
            {
                found = true;

                return std::string(val);
            }
            else
            {
                found = false;

                return std::string();
            }
        }

        bool FileExists(const std::string& path)
        {
            struct stat s;

            int res = stat(path.c_str(), &s);

            return res != -1;
        }
    }
}
