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
 * Declares ignite::IgniteConfiguration class.
 */

#ifndef _IGNITE_IGNITE_CONFIGURATION
#define _IGNITE_IGNITE_CONFIGURATION

#include <stdint.h>
#include <string>
#include <list>

namespace ignite
{
    /**
     * %Ignite configuration.
     */
    struct IgniteConfiguration
    {
        /** Path to Ignite home. */
        std::string igniteHome;

        /** Path to Spring configuration file. */
        std::string springCfgPath;

        /** Path ot JVM libbrary. */
        std::string jvmLibPath;

        /** JVM classpath. */
        std::string jvmClassPath;

        /** Initial amount of JVM memory. */
        int32_t jvmInitMem;

        /** Maximum amount of JVM memory. */
        int32_t jvmMaxMem;

        /** Additional JVM options. */
        std::list<std::string> jvmOpts;

        /**
         * Default constructor.
         */
        IgniteConfiguration() : igniteHome(), springCfgPath(), jvmLibPath(), jvmClassPath(),
            jvmInitMem(512), jvmMaxMem(1024), jvmOpts()
        {
            // No-op.
        }
    };    
}

#endif //_IGNITE_IGNITE_CONFIGURATION