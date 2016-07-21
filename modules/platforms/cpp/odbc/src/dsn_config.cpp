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

#include "ignite/odbc/utility.h"
#include "ignite/odbc/system/odbc_constants.h"

#include "ignite/odbc/dsn_config.h"

using ignite::odbc::config::Configuration;

#define BUFFER_SIZE 1024
#define CONFIG_FILE "ODBC.INI"

namespace ignite
{
    namespace odbc
    {
        void ThrowLastSetupError()
        {
            DWORD code;
            char msg[BUFFER_SIZE];

            SQLInstallerError(1, &code, msg, sizeof(msg), NULL);

            std::stringstream buf;

            buf << "Message: \"" << msg << "\", Code: " << code;

            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
        }

        void WriteDsnString(const char* dsn, const char* key, const char* value)
        {
            if (!SQLWritePrivateProfileString(dsn, key, value, CONFIG_FILE))
                ThrowLastSetupError();
        }

        std::string ReadDsnString(const char* dsn, const char* key, const char* dflt)
        {
            char buf[BUFFER_SIZE];

            memset(buf, 0, sizeof(buf));

            SQLGetPrivateProfileString(dsn, key, dflt, buf, sizeof(buf), CONFIG_FILE);

            return std::string(buf);
        }

        void ReadDsnConfiguration(const char* dsn, Configuration& config)
        {
            using namespace config;
            using common::LexicalCast;

            std::string host = ReadDsnString(dsn, attrkey::host.c_str(), config.GetHost().c_str());
            std::string port = ReadDsnString(dsn, attrkey::port.c_str(), LexicalCast<std::string>(config.GetTcpPort()).c_str());
            std::string cache = ReadDsnString(dsn, attrkey::cache.c_str(), config.GetCache().c_str());

            config.SetHost(host);
            config.SetTcpPort(LexicalCast<uint16_t>(port));
            config.SetCache(cache);
        }
    }
}