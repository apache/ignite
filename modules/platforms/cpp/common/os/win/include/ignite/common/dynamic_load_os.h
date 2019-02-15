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
