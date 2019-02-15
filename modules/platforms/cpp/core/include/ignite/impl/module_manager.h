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

#ifndef _IGNITE_IMPL_MODULE_MANAGER
#define _IGNITE_IMPL_MODULE_MANAGER

#include <vector>
#include <sstream>

#include <ignite/common/common.h>
#include <ignite/common/dynamic_load_os.h>
#include <ignite/ignite_binding_context.h>

/**
 * @def IGNITE_CACHE_ENTRY_PROCESSOR_INVOKER_NAME
 * Function name in which user Invoke callbacks are registred.
 */
#define IGNITE_MODULE_INIT_CALLBACK_NAME "IgniteModuleInit"

/**
 * Max number of additional init callbacks
 */
#define IGNITE_MODULE_ADDITIONAL_INIT_CALLBACKS_MAX_NUM 100

#define IGNITE_EXPORTED_CALL \
    extern "C" IGNITE_IMPORT_EXPORT

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
            typedef common::dynamic::Module Module;
            typedef void (ModuleInitCallback)(IgniteBindingContext&);

        public:
            /**
             * Constructor.
             *
             * @param context Ignite binding context.
             */
            ModuleManager(const IgniteBindingContext& context) :
                loadedModules(),
                bindingContext(context)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~ModuleManager()
            {
                for (std::vector<Module>::iterator it = loadedModules.begin(); it != loadedModules.end(); ++it)
                    it->Unload();
            }

            /**
             * Load module.
             *
             * @param path Module path.
             * @param err Error.
             */
            void LoadModule(const std::string& path, IgniteError& err)
            {
                common::dynamic::Module module = common::dynamic::LoadModule(path);

                if (!module.IsLoaded())
                {
                    err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                        ("Can not load module [path=" + path + ']').c_str());

                    return;
                }

                RegisterModule(module);
            }

            /**
             * Register module in ModuleManager.
             *
             * @param module Module to register.
             */
            void RegisterModule(Module& module)
            {
                loadedModules.push_back(module);

                ModuleInitCallback* callback = GetModuleInitCallback(module);

                if (callback)
                    callback(bindingContext);

                for (int i = 0; i < IGNITE_MODULE_ADDITIONAL_INIT_CALLBACKS_MAX_NUM; ++i)
                {
                    ModuleInitCallback* callback0 = GetAdditionalModuleInitCallback(module, i);

                    if (!callback0)
                        break;

                    callback0(bindingContext);
                }
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(ModuleManager);

            /**
             * Get callback that inits Ignite module.
             *
             * @param module Module for which callback should be retrieved.
             * @return Callback if found and null-pointer otherwise.
             */
            static ModuleInitCallback* GetModuleInitCallback(Module& module)
            {
                return reinterpret_cast<ModuleInitCallback*>(
                    module.FindSymbol(IGNITE_MODULE_INIT_CALLBACK_NAME));
            }

            static ModuleInitCallback* GetAdditionalModuleInitCallback(Module& module, int num)
            {
                std::stringstream tmp;

                tmp << IGNITE_MODULE_INIT_CALLBACK_NAME << num;

                return reinterpret_cast<ModuleInitCallback*>(
                    module.FindSymbol(tmp.str()));
            }

            /** Collection of loaded modules. */
            std::vector<Module> loadedModules;

            /** Ignite environment. */
            IgniteBindingContext bindingContext;
        };
    }
}

#endif //_IGNITE_IMPL_MODULE_MANAGER
