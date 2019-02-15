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
#ifndef _IGNITE_COMMON_LAZY
#define _IGNITE_COMMON_LAZY

#include <stdint.h>

#include <ignite/common/concurrent.h>
#include <ignite/common/common.h>

namespace ignite
{
    namespace common
    {
        /**
         * Lazy initialization template.
         */
        template<typename T>
        class IGNITE_IMPORT_EXPORT Lazy
        {
            /**
             * Init function type.
             */
            struct InitFunctionType
            {
                /**
                 * Destructor.
                 */
                virtual ~InitFunctionType()
                {
                    // No-op.
                }

                /**
                 * Call init function.
                 *
                 * @return Init function.
                 */
                virtual T* Init() = 0;
            };

            /**
             * Init function type.
             */
            template<typename F>
            struct InitFunctionType0 : InitFunctionType
            {
                /**
                 * Constructor.
                 *
                 * @param func Function.
                 */
                InitFunctionType0(F func) :
                    func(func)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~InitFunctionType0()
                {
                    // No-op.
                }

                /**
                 * Call init function.
                 *
                 * @return Init function.
                 */
                virtual T* Init()
                {
                    return func();
                }

            private:
                /** Init function. */
                F func;
            };

        public:
            typedef T InstanceType;

            /**
             * Constructor.
             *
             * @param initFunc Initialization function.
             */
            template<typename F>
            Lazy(F initFunc) :
                initFunc(new InitFunctionType0<F>(initFunc)),
                instance(),
                lock()
            {
                // No-op.
            }

            /**
             * Default constructor for late init.
             */
            Lazy() :
                initFunc(),
                instance(),
                lock()
            {
                // No-op.
            }

            /**
             * Init function. Can be used for late init.
             *
             * @warning Do not re-init inited instances to avoid undefined behaviour.
             *
             * @param initFunc Initialization function.
             */
            template<typename F>
            void Init(F initFunc)
            {
                this->initFunc = concurrent::SharedPointer<InitFunctionType>(new InitFunctionType0<F>(initFunc));
            }

            /**
             * Get instance.
             *
             * Inits if was not inited prior.
             *
             * @return Instance.
             */
            concurrent::SharedPointer<InstanceType> Get()
            {
                if (instance.Get())
                    return instance;

                concurrent::CsLockGuard guard(lock);

                if (instance.Get())
                    return instance;

                instance = concurrent::SharedPointer<InstanceType>(initFunc.Get()->Init());

                return instance;
            }

        private:
            /** Init function. */
            concurrent::SharedPointer<InitFunctionType> initFunc;

            /** Instance. */
            concurrent::SharedPointer<InstanceType> instance;

            /** Sync lock. */
            concurrent::CriticalSection lock;
        };
    }
}

#endif // _IGNITE_COMMON_LAZY