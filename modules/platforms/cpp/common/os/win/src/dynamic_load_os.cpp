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

#include <sstream>

#include "ignite/common/dynamic_load_os.h"

namespace
{
    std::wstring stringToWstring(const std::string& str)
    {
        int wslen = MultiByteToWideChar(CP_UTF8, 0, str.c_str(), static_cast<int>(str.size()), NULL, 0);

        wchar_t *converted = reinterpret_cast<wchar_t*>(alloca(wslen * sizeof(wchar_t)));

        MultiByteToWideChar(CP_UTF8, 0, str.c_str(), static_cast<int>(str.size()), converted, wslen);

        std::wstring res(converted, wslen);

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

            void* Module::FindSymbol(const std::string& name)
            {
                return FindSymbol(name.c_str());
            }

            bool Module::IsLoaded() const
            {
                return handle != NULL;
            }

            Module LoadModule(const char* path)
            {
                std::string strPath(path);

                return LoadModule(strPath);
            }

            Module LoadModule(std::string& path)
            {
                std::wstring convertedPath = stringToWstring(path);

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