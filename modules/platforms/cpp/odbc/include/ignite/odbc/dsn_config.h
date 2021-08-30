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

#ifndef _IGNITE_ODBC_DSN_CONFIG
#define _IGNITE_ODBC_DSN_CONFIG

#include "ignite/odbc/config/configuration.h"

namespace ignite
{
    namespace odbc
    {
        /**
         * Extract last setup error and throw it like IgniteError.
         */
        void ThrowLastSetupError();

        /**
         * Add new string to the DSN file.
         *
         * @param dsn DSN name.
         * @param key Key.
         * @param value Value.
         */
        void WriteDsnString(const char* dsn, const char* key, const char* value);

        /**
         * Get string from the DSN file.
         *
         * @param dsn DSN name.
         * @param key Key.
         * @param dflt Default value.
         * @return Value.
         */
        std::string ReadDsnString(const char* dsn, const char* key, const char* dflt);

        /**
         * Read DSN to fill the configuration.
         *
         * @param dsn DSN name.
         * @param config Configuration.
         * @param diag Diagnostic collector.
         */
        void ReadDsnConfiguration(const char* dsn, config::Configuration& config, diagnostic::DiagnosticRecordStorage *diag);
    }
}

#endif //_IGNITE_ODBC_DSN_CONFIG
