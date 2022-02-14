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

#include <ignite/common/fixed_size_array.h>

#include "ignite/odbc/utility.h"
#include "ignite/odbc/config/connection_string_parser.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/dsn_config.h"
#include "ignite/odbc/config/config_tools.h"


using namespace ignite::odbc::config;

#define BUFFER_SIZE (1024 * 1024)
#define CONFIG_FILE "ODBC.INI"

namespace ignite
{
    namespace odbc
    {
        void ThrowLastSetupError()
        {
            DWORD code;
            common::FixedSizeArray<char> msg(BUFFER_SIZE);

            SQLInstallerError(1, &code, msg.GetData(), msg.GetSize(), NULL);

            std::stringstream buf;

            buf << "Message: \"" << msg.GetData() << "\", Code: " << code;

            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
        }

        void WriteDsnString(const char* dsn, const char* key, const char* value)
        {
            if (!SQLWritePrivateProfileString(dsn, key, value, CONFIG_FILE))
                ThrowLastSetupError();
        }

        SettableValue<std::string> ReadDsnString(const char* dsn, const std::string& key, const std::string& dflt = "")
        {
            static const char* unique = "35a920dd-8837-43d2-a846-e01a2e7b5f84";

            SettableValue<std::string> val(dflt);

            common::FixedSizeArray<char> buf(BUFFER_SIZE);

            int ret = SQLGetPrivateProfileString(dsn, key.c_str(), unique, buf.GetData(), buf.GetSize(), CONFIG_FILE);

            if (ret > BUFFER_SIZE)
            {
                buf.Reset(ret + 1);

                ret = SQLGetPrivateProfileString(dsn, key.c_str(), unique, buf.GetData(), buf.GetSize(), CONFIG_FILE);
            }

            std::string res(buf.GetData());

            if (res != unique)
                val.SetValue(res);

            return val;
        }

        SettableValue<int32_t> ReadDsnInt(const char* dsn, const std::string& key, int32_t dflt = 0)
        {
            SettableValue<std::string> str = ReadDsnString(dsn, key, "");

            SettableValue<int32_t> res(dflt);

            if (str.IsSet())
                res.SetValue(common::LexicalCast<int, std::string>(str.GetValue()));

            return res;
        }

        SettableValue<bool> ReadDsnBool(const char* dsn, const std::string& key, bool dflt = false)
        {
            SettableValue<std::string> str = ReadDsnString(dsn, key, "");

            SettableValue<bool> res(dflt);

            if (str.IsSet())
                res.SetValue(str.GetValue() == "true");

            return res;
        }

        void ReadDsnConfiguration(const char* dsn, Configuration& config, diagnostic::DiagnosticRecordStorage* diag)
        {
            SettableValue<std::string> address = ReadDsnString(dsn, ConnectionStringParser::Key::address);

            if (address.IsSet() && !config.IsAddressesSet())
            {
                std::vector<EndPoint> endPoints;

                ParseAddress(address.GetValue(), endPoints, diag);

                config.SetAddresses(endPoints);
            }

            SettableValue<std::string> server = ReadDsnString(dsn, ConnectionStringParser::Key::server);

            if (server.IsSet() && !config.IsHostSet())
                config.SetHost(server.GetValue());

            SettableValue<int32_t> port = ReadDsnInt(dsn, ConnectionStringParser::Key::port);

            if (port.IsSet() && !config.IsTcpPortSet())
                config.SetTcpPort(static_cast<uint16_t>(port.GetValue()));

            SettableValue<std::string> schema = ReadDsnString(dsn, ConnectionStringParser::Key::schema);

            if (schema.IsSet() && !config.IsSchemaSet())
                config.SetSchema(schema.GetValue());

            SettableValue<bool> distributedJoins = ReadDsnBool(dsn, ConnectionStringParser::Key::distributedJoins);

            if (distributedJoins.IsSet() && !config.IsDistributedJoinsSet())
                config.SetDistributedJoins(distributedJoins.GetValue());

            SettableValue<bool> enforceJoinOrder = ReadDsnBool(dsn, ConnectionStringParser::Key::enforceJoinOrder);

            if (enforceJoinOrder.IsSet() && !config.IsEnforceJoinOrderSet())
                config.SetEnforceJoinOrder(enforceJoinOrder.GetValue());

            SettableValue<bool> replicatedOnly = ReadDsnBool(dsn, ConnectionStringParser::Key::replicatedOnly);

            if (replicatedOnly.IsSet() && !config.IsReplicatedOnlySet())
                config.SetReplicatedOnly(replicatedOnly.GetValue());

            SettableValue<bool> collocated = ReadDsnBool(dsn, ConnectionStringParser::Key::collocated);

            if (collocated.IsSet() && !config.IsCollocatedSet())
                config.SetCollocated(collocated.GetValue());

            SettableValue<bool> lazy = ReadDsnBool(dsn, ConnectionStringParser::Key::lazy);

            if (lazy.IsSet() && !config.IsLazySet())
                config.SetLazy(lazy.GetValue());

            SettableValue<bool> skipReducerOnUpdate = ReadDsnBool(dsn, ConnectionStringParser::Key::skipReducerOnUpdate);

            if (skipReducerOnUpdate.IsSet() && !config.IsSkipReducerOnUpdateSet())
                config.SetSkipReducerOnUpdate(skipReducerOnUpdate.GetValue());

            SettableValue<std::string> versionStr = ReadDsnString(dsn, ConnectionStringParser::Key::protocolVersion);

            if (versionStr.IsSet() && !config.IsProtocolVersionSet())
            {
                ProtocolVersion version = ProtocolVersion::FromString(versionStr.GetValue());

                if (!version.IsSupported())
                    version = Configuration::DefaultValue::protocolVersion;

                config.SetProtocolVersion(version);
            }

            SettableValue<int32_t> pageSize = ReadDsnInt(dsn, ConnectionStringParser::Key::pageSize);

            if (pageSize.IsSet() && !config.IsPageSizeSet() && pageSize.GetValue() > 0)
                config.SetPageSize(pageSize.GetValue());

            SettableValue<std::string> sslModeStr = ReadDsnString(dsn, ConnectionStringParser::Key::sslMode);

            if (sslModeStr.IsSet() && !config.IsSslModeSet())
            {
                ssl::SslMode::Type sslMode = ssl::SslMode::FromString(sslModeStr.GetValue(), ssl::SslMode::DISABLE);

                config.SetSslMode(sslMode);
            }

            SettableValue<std::string> sslKeyFile = ReadDsnString(dsn, ConnectionStringParser::Key::sslKeyFile);

            if (sslKeyFile.IsSet() && !config.IsSslKeyFileSet())
                config.SetSslKeyFile(sslKeyFile.GetValue());

            SettableValue<std::string> sslCertFile = ReadDsnString(dsn, ConnectionStringParser::Key::sslCertFile);

            if (sslCertFile.IsSet() && !config.IsSslCertFileSet())
                config.SetSslCertFile(sslCertFile.GetValue());

            SettableValue<std::string> sslCaFile = ReadDsnString(dsn, ConnectionStringParser::Key::sslCaFile);

            if (sslCaFile.IsSet() && !config.IsSslCaFileSet())
                config.SetSslCaFile(sslCaFile.GetValue());

            SettableValue<std::string> user = ReadDsnString(dsn, ConnectionStringParser::Key::user);

            if (user.IsSet() && !config.IsUserSet())
                config.SetUser(user.GetValue());

            SettableValue<std::string> password = ReadDsnString(dsn, ConnectionStringParser::Key::password);

            if (password.IsSet() && !config.IsPasswordSet())
                config.SetPassword(password.GetValue());

            SettableValue<std::string> nestedTxModeStr = ReadDsnString(dsn, ConnectionStringParser::Key::nestedTxMode);

            if (nestedTxModeStr.IsSet() && !config.IsNestedTxModeSet())
                config.SetNestedTxMode(NestedTxMode::FromString(nestedTxModeStr.GetValue(), config.GetNestedTxMode()));
        }
    }
}
