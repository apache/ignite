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

#include <sstream>
#include <vector>

#include "ignite/common/dynamic_load_os.h"

namespace
{
    std::wstring StringToWstring(const std::string& str)
    {
        int wslen = MultiByteToWideChar(CP_UTF8, 0, str.c_str(), static_cast<int>(str.size()), NULL, 0);

        if (!wslen)
            return std::wstring();

        std::vector<WCHAR> converted(wslen);

        MultiByteToWideChar(CP_UTF8, 0, str.c_str(), static_cast<int>(str.size()), &converted[0], wslen);

        std::wstring res(converted.begin(), converted.end());

        return res;
    }
}

namespace ignite
{
    namespace common
    {
        namespace dynamic
        {
            Module::Module() : handle(NULL)
            {
                // No-op.
            }

            Module::Module(HMODULE handle) : handle(handle)
            {
                // No-op.
            }

            Module::~Module()
            {
                // No-op.
            }

            Module::Module(const Module& other) : handle(other.handle)
            {
                // No-op.
            }

            Module& Module::operator=(const Module& other)
            {
                handle = other.handle;

                return *this;
            }

            void* Module::FindSymbol(const char* name)
            {
                return GetProcAddress(handle, name);
            }

            bool Module::IsLoaded() const
            {
                return handle != NULL;
            }

            void Module::Unload()
            {
                if (IsLoaded())
                {
                    FreeLibrary(handle);

                    handle = NULL;
                }
            }

            Module LoadModule(const char* path)
            {
                std::string strPath(path);

                return LoadModule(strPath);
            }

            Module LoadModule(const std::string& path)
            {
                std::wstring convertedPath = StringToWstring(path);

                HMODULE handle = LoadLibrary(convertedPath.c_str());

                return Module(handle);
            }

            Module GetCurrent()
            {
                HMODULE handle = GetModuleHandle(NULL);

                return Module(handle);
            }
        }
    }
}