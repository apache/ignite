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
#include <vector>

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

        std::string GetEnv(const std::string& name)
        {
            static const std::string empty;

            return GetEnv(name, empty);
        }

        std::string GetEnv(const std::string& name, const std::string& dflt)
        {
            char res[32767];

            DWORD envRes = GetEnvironmentVariableA(name.c_str(), res, sizeof(res) / sizeof(res[0]));

            if (envRes == 0 || envRes > sizeof(res))
                return dflt;

            return std::string(res, static_cast<size_t>(envRes));
        }

        bool FileExists(const std::string& path)
        {
            WIN32_FIND_DATAA findres;

            HANDLE hnd = FindFirstFileA(path.c_str(), &findres);

            if (hnd == INVALID_HANDLE_VALUE)
                return false;

            FindClose(hnd);

            return true;
        }

        bool IsValidDirectory(const std::string& path)
        {
            if (path.empty())
                return false;

            DWORD attrs = GetFileAttributesA(path.c_str());

            return attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY) != 0;
        }

        bool DeletePath(const std::string& path)
        {
            std::vector<TCHAR> path0(path.begin(), path.end());
            path0.push_back('\0');
            path0.push_back('\0');

            SHFILEOPSTRUCT fileop;
            fileop.hwnd = NULL;
            fileop.wFunc = FO_DELETE;
            fileop.pFrom = &path0[0];
            fileop.pTo = NULL;
            fileop.fFlags = FOF_NOCONFIRMATION | FOF_SILENT;

            fileop.fAnyOperationsAborted = FALSE;
            fileop.lpszProgressTitle = NULL;
            fileop.hNameMappings = NULL;

            int ret = SHFileOperation(&fileop);

            return ret == 0;
        }

        StdCharOutStream& Fs(StdCharOutStream& ostr)
        {
            ostr.put('\\');
            return ostr;
        }

        StdCharOutStream& Dle(StdCharOutStream& ostr)
        {
            static const char expansion[] = ".dll";

            ostr.write(expansion, sizeof(expansion) - 1);

            return ostr;
        }

        IGNITE_IMPORT_EXPORT unsigned GetRandSeed()
        {
            return static_cast<unsigned>(GetTickCount() ^ GetCurrentProcessId());
        }
    }
}
