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

#ifndef _IGNITE_CONFIGURATION
#define _IGNITE_CONFIGURATION

#include <stdint.h>

namespace ignite
{    
    /**
     * Single JVM option.
     */
    struct IgniteJvmOption
    {
        /** Option. */
        char* opt;

        /**
         * Default constructor.
         */
        IgniteJvmOption() : opt(NULL)
        {
            // No-op.    
        }

        /**
         * Constructor.
         *
         * @param opt Option.
         */
        IgniteJvmOption(char* opt) : opt(opt)
        {
            // No-op.
        }
    };

    /**
     * Ignite configuration.
     */
    struct IgniteConfiguration
    {
        /** Path to Ignite home. */
        char* igniteHome;

        /** Path to Spring configuration file. */
        char* springCfgPath;

        /** Path ot JVM libbrary. */
        char* jvmLibPath;

        /** JVM classpath. */
        char* jvmClassPath;

        /** Initial amount of JVM memory. */
        int32_t jvmInitMem;

        /** Maximum amount of JVM memory. */
        int32_t jvmMaxMem;

        /** Additional JVM options. */
        IgniteJvmOption* jvmOpts;

        /** Additional JVM options count. */
        int32_t jvmOptsLen;

        /**
         * Constructor.
         */
        IgniteConfiguration() : igniteHome(NULL), springCfgPath(NULL), jvmLibPath(NULL), jvmClassPath(NULL),
            jvmInitMem(512), jvmMaxMem(1024), jvmOpts(NULL), jvmOptsLen(0)
        {
            // No-op.
        }
    };    
}

#endif