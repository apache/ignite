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
#include "ignite/odbc/config/connection_string_parser.h"
#include "ignite/odbc/system/odbc_constants.h"

#include "ignite/odbc/dsn_config.h"
#include "ignite/odbc/config/config_tools.h"


using ignite::odbc::config::Configuration;
using ignite::odbc::config::ConnectionStringParser;

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
            std::string address = ReadDsnString(dsn, ConnectionStringParser::Key::address,
                Configuration::DefaultValue::address);

            std::string server = ReadDsnString(dsn, ConnectionStringParser::Key::server,
                Configuration::DefaultValue::server);

            uint16_t port = ReadDsnInt(dsn, ConnectionStringParser::Key::port, Configuration::DefaultValue::port);

            std::string schema = ReadDsnString(dsn, ConnectionStringParser::Key::schema,
                Configuration::DefaultValue::schema);

            bool distributedJoins = ReadDsnBool(dsn, ConnectionStringParser::Key::distributedJoins,
                Configuration::DefaultValue::distributedJoins);

            bool enforceJoinOrder = ReadDsnBool(dsn, ConnectionStringParser::Key::enforceJoinOrder,
                Configuration::DefaultValue::enforceJoinOrder);

            bool replicatedOnly = ReadDsnBool(dsn, ConnectionStringParser::Key::replicatedOnly,
                Configuration::DefaultValue::replicatedOnly);

            bool collocated = ReadDsnBool(dsn, ConnectionStringParser::Key::collocated,
                Configuration::DefaultValue::collocated);

            bool lazy = ReadDsnBool(dsn, ConnectionStringParser::Key::lazy, Configuration::DefaultValue::lazy);

            bool skipReducerOnUpdate = ReadDsnBool(dsn, ConnectionStringParser::Key::skipReducerOnUpdate,
                Configuration::DefaultValue::skipReducerOnUpdate);

            std::string versionStr = ReadDsnString(dsn, ConnectionStringParser::Key::protocolVersion,
                Configuration::DefaultValue::protocolVersion.ToString());

            int32_t pageSize = ReadDsnInt(dsn, ConnectionStringParser::Key::pageSize,
                Configuration::DefaultValue::pageSize);

            if (pageSize <= 0)
                pageSize = config.GetPageSize();

            std::string sslModeStr = ReadDsnString(dsn, ConnectionStringParser::Key::sslMode,
                ssl::SslMode::ToString(Configuration::DefaultValue::sslMode));

            std::string sslKeyFile = ReadDsnString(dsn, ConnectionStringParser::Key::sslKeyFile,
                Configuration::DefaultValue::sslKeyFile);

            std::string sslCertFile = ReadDsnString(dsn, ConnectionStringParser::Key::sslCertFile,
                Configuration::DefaultValue::sslCertFile);

            std::string sslCaFile = ReadDsnString(dsn, ConnectionStringParser::Key::sslCaFile,
                Configuration::DefaultValue::sslCaFile);

            std::vector<EndPoint> endPoints;
            config::ParseAddress(address, endPoints, 0);

            ProtocolVersion version = ProtocolVersion::FromString(versionStr);

            if (!version.IsSupported())
                version = Configuration::DefaultValue::protocolVersion;

            ssl::SslMode::Type sslMode = ssl::SslMode::FromString(sslModeStr, ssl::SslMode::DISABLE);

            config.SetAddresses(endPoints);
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