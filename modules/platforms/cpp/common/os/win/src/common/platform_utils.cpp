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

#include <windows.h>

#include <ignite/common/platform_utils.h>

namespace ignite
{
    namespace common
    {
        time_t IgniteTimeGm(const tm& time)
        {
            tm tmc = time;

            return _mkgmtime(&tmc);
        }

        time_t IgniteTimeLocal(const tm& time)
        {
            tm tmc = time;

            return mktime(&tmc);
        }

        bool IgniteGmTime(time_t in, tm& out)
        {
            return gmtime_s(&out, &in) == 0;
        }

        bool IgniteLocalTime(time_t in, tm& out)
        {
            return localtime_s(&out, &in) == 0;
        }

        std::string GetEnv(const std::string& name, bool& found)
        {
            char res0[32767];

            DWORD envRes = GetEnvironmentVariableA(name.c_str(), res0, 32767);

            if (envRes != 0)
            {
                found = true;

                return std::string(res0);
            }
            else
            {
                found = false;

                return std::string();
            }
        }

        bool FileExists(const std::string& path)
        {
            WIN32_FIND_DATAA findres;

            HANDLE hnd = FindFirstFileA(path.c_str(), &findres);

            if (hnd == INVALID_HANDLE_VALUE)
                return false;
            else
            {
                FindClose(hnd);

                return true;
            }
        }
    }
}
