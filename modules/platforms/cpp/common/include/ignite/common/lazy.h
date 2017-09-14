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