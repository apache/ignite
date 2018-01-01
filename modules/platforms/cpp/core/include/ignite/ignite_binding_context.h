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

/**
 * @file
 * Declares ignite::IgniteBindingContext class.
 */

#ifndef _IGNITE_IGNITE_BINDING_CONTEXT
#define _IGNITE_IGNITE_BINDING_CONTEXT

#include <ignite/ignite_binding.h>
#include <ignite/ignite_configuration.h>

namespace ignite
{
    namespace impl
    {
        class IgniteEnvironment;
    }

    /**
     * %Ignite binding context.
     *
     * Provides methods that can be used to get Ignite components which may be
     * needed for initial module initialization.
     */
    class IgniteBindingContext
    {
        friend class impl::IgniteEnvironment;
    public:
        /**
         * Get binding.
         *
         * @return IgniteBinding instance.
         */
        IgniteBinding GetBinding() const
        {
            return binding;
        }

        /**
         * Get configuration for current node.
         *
         * @return Configuration.
         */
        const IgniteConfiguration& GetConfiguration() const
        {
            return cfg;
        }

    private:
        /**
         * Constructor.
         *
         * @param cfg Configuration.
         * @param binding Binding.
         */
        IgniteBindingContext(const IgniteConfiguration& cfg, const IgniteBinding& binding) :
            cfg(cfg),
            binding(binding)
        {
            // No-op.
        }

        /** Configuration */
        const IgniteConfiguration& cfg;

        /** Binding. */
        IgniteBinding binding;
    };
}

#endif //_IGNITE_IGNITE_BINDING_CONTEXT