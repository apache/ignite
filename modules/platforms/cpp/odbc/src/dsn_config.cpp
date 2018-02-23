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

#include <set>

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

        std::string ReadDsnString(const char* dsn, const std::string& key, const std::string& dflt)
        {
            char buf[BUFFER_SIZE];

            memset(buf, 0, sizeof(buf));

            SQLGetPrivateProfileString(dsn, key.c_str(), dflt.c_str(), buf, sizeof(buf), CONFIG_FILE);

            return std::string(buf);
        }

        int ReadDsnInt(const char* dsn, const std::string& key, int dflt)
        {
            char buf[BUFFER_SIZE];

            memset(buf, 0, sizeof(buf));

            std::string dflt0 = common::LexicalCast<std::string>(dflt);

            SQLGetPrivateProfileString(dsn, key.c_str(), dflt0.c_str(), buf, sizeof(buf), CONFIG_FILE);

            return common::LexicalCast<int, std::string>(buf);
        }

        bool ReadDsnBool(const char* dsn, const std::string& key, bool dflt)
        {
            char buf[BUFFER_SIZE];

            memset(buf, 0, sizeof(buf));

            std::string dflt0 = dflt ? "true" : "false";

            SQLGetPrivateProfileString(dsn, key.c_str(), dflt0.c_str(), buf, sizeof(buf), CONFIG_FILE);

            return std::string(buf) == "true";
        }

        void ReadDsnConfiguration(const char* dsn, Configuration& config)
        {
            std::string address = ReadDsnString(dsn, Configuration::Key::address, config.GetAddress());

            std::string server = ReadDsnString(dsn, Configuration::Key::server, config.GetHost());

            uint16_t port = ReadDsnInt(dsn, Configuration::Key::port, config.GetTcpPort());

            std::string schema = ReadDsnString(dsn, Configuration::Key::schema, config.GetSchema());

            bool distributedJoins = ReadDsnBool(dsn, Configuration::Key::distributedJoins, config.IsDistributedJoins());

            bool enforceJoinOrder = ReadDsnBool(dsn, Configuration::Key::enforceJoinOrder, config.IsEnforceJoinOrder());

            bool replicatedOnly = ReadDsnBool(dsn, Configuration::Key::replicatedOnly, config.IsReplicatedOnly());

            bool collocated = ReadDsnBool(dsn, Configuration::Key::collocated, config.IsCollocated());

            bool lazy = ReadDsnBool(dsn, Configuration::Key::lazy, config.IsLazy());

            bool skipReducerOnUpdate =
                ReadDsnBool(dsn, Configuration::Key::skipReducerOnUpdate, config.IsSkipReducerOnUpdate());

            std::string version = ReadDsnString(dsn, Configuration::Key::protocolVersion,
                config.GetProtocolVersion().ToString());

            int32_t pageSize = ReadDsnInt(dsn, Configuration::Key::pageSize, config.GetPageSize());

            if (pageSize <= 0)
                pageSize = config.GetPageSize();

            std::string sslMode = ReadDsnString(dsn, Configuration::Key::sslMode, config.GetSslMode());

            std::string sslKeyFile = ReadDsnString(dsn, Configuration::Key::sslKeyFile, config.GetSslKeyFile());

            std::string sslCertFile = ReadDsnString(dsn, Configuration::Key::sslCertFile, config.GetSslCertFile());

            std::string sslCaFile = ReadDsnString(dsn, Configuration::Key::sslCaFile, config.GetSslCaFile());

            config.SetAddress(address);
            config.SetHost(server);
            config.SetTcpPort(port);
            config.SetSchema(schema);
            config.SetDistributedJoins(distributedJoins);
            config.SetEnforceJoinOrder(enforceJoinOrder);
            config.SetReplicatedOnly(replicatedOnly);
            config.SetCollocated(collocated);
            config.SetLazy(lazy);
            config.SetSkipReducerOnUpdate(skipReducerOnUpdate);
            config.SetProtocolVersion(version);
            config.SetPageSize(pageSize);
            config.SetSslMode(sslMode);
            config.SetSslKeyFile(sslKeyFile);
            config.SetSslCertFile(sslCertFile);
            config.SetSslCaFile(sslCaFile);
        }
    }
}