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

#ifndef _IGNITE_COMMON_DYNAMIC_LOAD
#define _IGNITE_COMMON_DYNAMIC_LOAD

#include <Windows.h>

#include <string>

#include "ignite/common/common.h"

namespace ignite
{
    namespace common
    {
        namespace dynamic
        {
            /**
             * Represents dynamically loadable program module such as dymanic
             * or shared library.
             */
            class IGNITE_IMPORT_EXPORT Module
            {
            public:
                /**
                 * Default constructor.
                 */
                Module();

                /**
                 * Handle constructor.
                 *
                 * @param handle Os-specific Module handle.
                 */
                Module(HMODULE handle);

                /**
                 * Destructor.
                 */
                ~Module();

                /**
                 * Copy constructor.
                 *
                 * @param other Other instance.
                 */
                Module(const Module& other);

                /**
                 * Copy constructor.
                 *
                 * @param other Other instance.
                 * @return This.
                 */
                Module& operator=(const Module& other);

                /**
                 * Load symbol from Module.
                 *
                 * @param name Name of the symbol to load.
                 * @return Pointer to symbol if found and NULL otherwise.
                 */
                void* FindSymbol(const char* name);

                /**
                 * Load symbol from Module.
                 *
                 * @param name Name of the symbol to load.
                 * @return Pointer to symbol if found and NULL otherwise.
                 */
                void* FindSymbol(const std::string& name)
                {
                    return FindSymbol(name.c_str());
                }

                /**
                 * Check if the instance is loaded.
                 *
                 * @return True if the instance is loaded.
                 */
                bool IsLoaded() const;

                /**
                 * Unload module.
                 */
                void Unload();

            private:
                HMODULE handle;
            };

            /**
             * Load Module by the specified path.
             *
             * @param path Path to the Module to load.
             * @return Module instance.
             */
            IGNITE_IMPORT_EXPORT Module LoadModule(const char* path);

            /**
             * Load Module by the specified path.
             *
             * @param path Path to the Module to load.
             * @return Module instance.
             */
            IGNITE_IMPORT_EXPORT Module LoadModule(const std::string& path);

            /**
             * Returns Module associated with the calling process itself.
             *
             * @return Module for the calling process.
             */
            IGNITE_IMPORT_EXPORT Module GetCurrent();
        }
    }
}

#endif
