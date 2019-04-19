/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <dlfcn.h>

#include <sstream>
#include "ignite/common/dynamic_load_os.h"

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

            Module::Module(void* handle) : handle(handle)
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
                return dlsym(handle, name);
            }

            bool Module::IsLoaded() const
            {
                return handle != NULL;
            }

            void Module::Unload()
            {
                if (IsLoaded())
                    dlclose(handle);
            }

            Module LoadModule(const char* path)
            {
                void* handle = dlopen(path, RTLD_NOW);

                return Module(handle);
            }

            Module LoadModule(const std::string& path)
            {
                return LoadModule(path.c_str());
            }

            Module GetCurrent()
            {
                return LoadModule(NULL);
            }
        }
    }
}
