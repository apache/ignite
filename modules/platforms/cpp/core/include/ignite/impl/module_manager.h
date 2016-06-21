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

#ifndef _IGNITE_IMPL_MODULE_MANAGER
#define _IGNITE_IMPL_MODULE_MANAGER

#include <vector>

#include <ignite/common/common.h>
#include <ignite/common/dynamic_load_os.h>
#include "ignite/impl/invoke_manager.h"

namespace ignite
{
    namespace impl
    {
        /**
         * Module manager.
         * Provides methods to manipulate loadable modules.
         */
        class ModuleManager
        {
            typedef ignite::common::dynamic::Module Module;
            typedef void (InvokersRegisterCallback)(InvokeManager&);

        public:
            /**
             * Constructor.
             *
             * @param invokeMgr Invoke manager to use.
             */
            ModuleManager(InvokeManager* invokeMgr) :
                loadedModules(),
                invokeMgr(invokeMgr)
            {
                Module current = common::dynamic::GetCurrent();

                RegisterModule(current);
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(ModuleManager);

            /**
             * Register module in ModuleManager.
             *
             * @param module Module to register.
             */
            void RegisterModule(Module& module)
            {
                loadedModules.push_back(module);

                if (invokeMgr)
                {
                    InvokersRegisterCallback* callback = GetProcessorsRegisterCallback(module);

                    if (callback)
                        callback(*invokeMgr);
                }
            }

            /**
             * Get callback that registers cache entry processors for module.
             *
             * @param module Module for which callback should be retrieved.
             * @return Callback if found and null-pointer otherwise.
             */
            InvokersRegisterCallback* GetProcessorsRegisterCallback(Module& module)
            {
                return reinterpret_cast<InvokersRegisterCallback*>(
                    module.FindSymbol(IGNITE_CACHE_ENTRY_PROCESSOR_REGISTRATOR_NAME));
            }

            /** Collection of loaded modules. */
            std::vector<Module> loadedModules;

            /** Invoke manager reference. */
            InvokeManager* invokeMgr;
        };
    }
}

#endif //_IGNITE_IMPL_MODULE_MANAGER
