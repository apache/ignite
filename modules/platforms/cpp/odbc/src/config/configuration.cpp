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

#include <string>
#include <sstream>
#include <iterator>

#include "ignite/odbc/utility.h"
#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/config/connection_string_parser.h"
#include "ignite/odbc/config/config_tools.h"

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            const std::string Configuration::DefaultValue::dsn = "Apache Ignite DSN";
            const std::string Configuration::DefaultValue::driver = "Apache Ignite";
            const std::string Configuration::DefaultValue::schema = "PUBLIC";
            const std::string Configuration::DefaultValue::address = "";
            const std::string Configuration::DefaultValue::server = "";

            const uint16_t Configuration::DefaultValue::port = 10800;
            const int32_t Configuration::DefaultValue::pageSize = 1024;

            const bool Configuration::DefaultValue::distributedJoins = false;
            const bool Configuration::DefaultValue::enforceJoinOrder = false;
            const bool Configuration::DefaultValue::replicatedOnly = false;
            const bool Configuration::DefaultValue::collocated = false;
            const bool Configuration::DefaultValue::lazy = false;
            const bool Configuration::DefaultValue::skipReducerOnUpdate = false;

            const ProtocolVersion& Configuration::DefaultValue::protocolVersion = ProtocolVersion::GetCurrent();

            const ssl::SslMode::Type Configuration::DefaultValue::sslMode = ssl::SslMode::DISABLE;
            const std::string Configuration::DefaultValue::sslKeyFile = "";
            const std::string Configuration::DefaultValue::sslCertFile = "";
            const std::string Configuration::DefaultValue::sslCaFile = "";

            const std::string Configuration::DefaultValue::user = "";
            const std::string Configuration::DefaultValue::password = "";

            const NestedTxMode::Type Configuration::DefaultValue::nestedTxMode = NestedTxMode::AI_ERROR;

            Configuration::Configuration() :
                dsn(DefaultValue::dsn),
                driver(DefaultValue::driver),
                schema(DefaultValue::schema),
                server(DefaultValue::server),
                port(DefaultValue::port),
                pageSize(DefaultValue::pageSize),
                distributedJoins(DefaultValue::distributedJoins),
                enforceJoinOrder(DefaultValue::enforceJoinOrder),
                replicatedOnly(DefaultValue::replicatedOnly),
                collocated(DefaultValue::collocated),
                lazy(DefaultValue::lazy),
                skipReducerOnUpdate(DefaultValue::skipReducerOnUpdate),
                protocolVersion(DefaultValue::protocolVersion),
                endPoints(std::vector<EndPoint>()),
                sslMode(DefaultValue::sslMode),
                sslKeyFile(DefaultValue::sslKeyFile),
                sslCertFile(DefaultValue::sslCertFile),
                sslCaFile(DefaultValue::sslCaFile),
                user(DefaultValue::user),
                password(DefaultValue::password),
                nestedTxMode(DefaultValue::nestedTxMode)
            {
                // No-op.
            }

            Configuration::~Configuration()
            {
                // No-op.
            }

            std::string Configuration::ToConnectString() const
            {
                ArgumentMap arguments;

                ToMap(arguments);

                std::stringstream connect_string_buffer;

                for (ArgumentMap::const_iterator it = arguments.begin(); it != arguments.end(); ++it)
                {
                    const std::string& key = it->first;
                    const std::string& value = it->second;

                    if (value.empty())
                        continue;

                    if (value.find(' ') == std::string::npos)
                        connect_string_buffer << key << '=' << value << ';';
                    else
                        connect_string_buffer << key << "={" << value << "};";
                }

                return connect_string_buffer.str();
            }

            uint16_t Configuration::GetTcpPort() const
            {
                return port.GetValue();
            }

            void Configuration::SetTcpPort(uint16_t port)
            {
                this->port.SetValue(port);
            }

            bool Configuration::IsTcpPortSet() const
            {
                return port.IsSet();
            }

            const std::string& Configuration::GetDsn(const std::string& dflt) const
            {
                if (!dsn.IsSet())
                    return dflt;

                return dsn.GetValue();
            }

            bool Configuration::IsDsnSet() const
            {
                return dsn.IsSet();
            }

            void Configuration::SetDsn(const std::string& dsn)
            {
                this->dsn.SetValue(dsn);
            }

            const std::string& Configuration::GetDriver() const
            {
                return driver.GetValue();
            }

            void Configuration::SetDriver(const std::string& driver)
            {
                this->driver.SetValue(driver);
            }

            const std::string& Configuration::GetHost() const
            {
                return server.GetValue();
            }

            void Configuration::SetHost(const std::string& server)
            {
                this->server.SetValue(server);
            }

            bool Configuration::IsHostSet() const
            {
                return server.IsSet();
            }

            const std::string& Configuration::GetSchema() const
            {
                return schema.GetValue();
            }

            void Configuration::SetSchema(const std::string& schema)
            {
                this->schema.SetValue(schema);
            }

            bool Configuration::IsSchemaSet() const
            {
                return schema.IsSet();
            }

            const std::vector<EndPoint>& Configuration::GetAddresses() const
            {
                return endPoints.GetValue();
            }

            void Configuration::SetAddresses(const std::vector<EndPoint>& endPoints)
            {
                this->endPoints.SetValue(endPoints);
            }

            bool Configuration::IsAddressesSet() const
            {
                return endPoints.IsSet();
            }

            ssl::SslMode::Type Configuration::GetSslMode() const
            {
                return sslMode.GetValue();
            }

            void Configuration::SetSslMode(ssl::SslMode::Type sslMode)
            {
                this->sslMode.SetValue(sslMode);
            }

            bool Configuration::IsSslModeSet() const
            {
                return sslMode.IsSet();
            }

            const std::string& Configuration::GetSslKeyFile() const
            {
                return sslKeyFile.GetValue();
            }

            void Configuration::SetSslKeyFile(const std::string& sslKeyFile)
            {
                this->sslKeyFile.SetValue(sslKeyFile);
            }

            bool Configuration::IsSslKeyFileSet() const
            {
                return sslKeyFile.IsSet();
            }

            const std::string& Configuration::GetSslCertFile() const
            {
                return sslCertFile.GetValue();
            }

            void Configuration::SetSslCertFile(const std::string& sslCertFile)
            {
                this->sslCertFile.SetValue(sslCertFile);
            }

            bool Configuration::IsSslCertFileSet() const
            {
                return sslCertFile.IsSet();
            }

            const std::string& Configuration::GetSslCaFile() const
            {
                return sslCaFile.GetValue();
            }

            void Configuration::SetSslCaFile(const std::string& sslCaFile)
            {
                this->sslCaFile.SetValue(sslCaFile);
            }

            bool Configuration::IsSslCaFileSet() const
            {
                return sslCaFile.IsSet();
            }

            bool Configuration::IsDistributedJoins() const
            {
                return distributedJoins.GetValue();
            }

            void Configuration::SetDistributedJoins(bool val)
            {
                this->distributedJoins.SetValue(val);
            }

            bool Configuration::IsDistributedJoinsSet() const
            {
                return distributedJoins.IsSet();
            }

            bool Configuration::IsEnforceJoinOrder() const
            {
                return enforceJoinOrder.GetValue();
            }

            void Configuration::SetEnforceJoinOrder(bool val)
            {
                this->enforceJoinOrder.SetValue(val);
            }

            bool Configuration::IsEnforceJoinOrderSet() const
            {
                return enforceJoinOrder.IsSet();
            }

            bool Configuration::IsReplicatedOnly() const
            {
                return replicatedOnly.GetValue();
            }

            void Configuration::SetReplicatedOnly(bool val)
            {
                this->replicatedOnly.SetValue(val);
            }

            bool Configuration::IsReplicatedOnlySet() const
            {
                return replicatedOnly.IsSet();
            }

            bool Configuration::IsCollocated() const
            {
                return collocated.GetValue();
            }

            void Configuration::SetCollocated(bool val)
            {
                this->collocated.SetValue(val);
            }

            bool Configuration::IsCollocatedSet() const
            {
                return collocated.IsSet();
            }

            bool Configuration::IsLazy() const
            {
                return lazy.GetValue();
            }

            void Configuration::SetLazy(bool val)
            {
                this->lazy.SetValue(val);
            }

            bool Configuration::IsLazySet() const
            {
                return lazy.IsSet();
            }

            bool Configuration::IsSkipReducerOnUpdate() const
            {
                return skipReducerOnUpdate.GetValue();
            }

            void Configuration::SetSkipReducerOnUpdate(bool val)
            {
                this->skipReducerOnUpdate.SetValue(val);
            }

            bool Configuration::IsSkipReducerOnUpdateSet() const
            {
                return skipReducerOnUpdate.IsSet();
            }

            ProtocolVersion Configuration::GetProtocolVersion() const
            {
                return protocolVersion.GetValue();
            }

            void Configuration::SetProtocolVersion(const ProtocolVersion& version)
            {
                this->protocolVersion.SetValue(version);
            }

            bool Configuration::IsProtocolVersionSet() const
            {
                return protocolVersion.IsSet();
            }

            void Configuration::SetPageSize(int32_t size)
            {
                this->pageSize.SetValue(size);
            }

            bool Configuration::IsPageSizeSet() const
            {
                return pageSize.IsSet();
            }

            const std::string& Configuration::GetUser() const
            {
                return user.GetValue();
            }

            void Configuration::SetUser(const std::string& user)
            {
                this->user.SetValue(user);
            }

            bool Configuration::IsUserSet() const
            {
                return user.IsSet();
            }

            const std::string& Configuration::GetPassword() const
            {
                return password.GetValue();
            }

            void Configuration::SetPassword(const std::string& pass)
            {
                this->password.SetValue(pass);
            }

            bool Configuration::IsPasswordSet() const
            {
                return password.IsSet();
            }

            NestedTxMode::Type Configuration::GetNestedTxMode() const
            {
                return nestedTxMode.GetValue();
            }

            void Configuration::SetNestedTxMode(NestedTxMode::Type mode)
            {
                this->nestedTxMode.SetValue(mode);
            }

            bool Configuration::IsNestedTxModeSet() const
            {
                return nestedTxMode.IsSet();
            }

            int32_t Configuration::GetPageSize() const
            {
                return pageSize.GetValue();
            }

            void Configuration::ToMap(ArgumentMap& res) const
            {
                AddToMap(res, ConnectionStringParser::Key::dsn, dsn);
                AddToMap(res, ConnectionStringParser::Key::driver, driver);
                AddToMap(res, ConnectionStringParser::Key::schema, schema);
                AddToMap(res, ConnectionStringParser::Key::address, endPoints);
                AddToMap(res, ConnectionStringParser::Key::server, server);
                AddToMap(res, ConnectionStringParser::Key::port, port);
                AddToMap(res, ConnectionStringParser::Key::distributedJoins, distributedJoins);
                AddToMap(res, ConnectionStringParser::Key::enforceJoinOrder, enforceJoinOrder);
                AddToMap(res, ConnectionStringParser::Key::protocolVersion, protocolVersion);
                AddToMap(res, ConnectionStringParser::Key::pageSize, pageSize);
                AddToMap(res, ConnectionStringParser::Key::replicatedOnly, replicatedOnly);
                AddToMap(res, ConnectionStringParser::Key::collocated, collocated);
                AddToMap(res, ConnectionStringParser::Key::lazy, lazy);
                AddToMap(res, ConnectionStringParser::Key::skipReducerOnUpdate, skipReducerOnUpdate);
                AddToMap(res, ConnectionStringParser::Key::sslMode, sslMode);
                AddToMap(res, ConnectionStringParser::Key::sslKeyFile, sslKeyFile);
                AddToMap(res, ConnectionStringParser::Key::sslCertFile, sslCertFile);
                AddToMap(res, ConnectionStringParser::Key::sslCaFile, sslCaFile);
                AddToMap(res, ConnectionStringParser::Key::user, user);
                AddToMap(res, ConnectionStringParser::Key::password, password);
                AddToMap(res, ConnectionStringParser::Key::nestedTxMode, nestedTxMode);
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key, const SettableValue<uint16_t>& value)
            {
                if (value.IsSet())
                    map[key] = common::LexicalCast<std::string>(value.GetValue());
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key, const SettableValue<int32_t>& value)
            {
                if (value.IsSet())
                    map[key] = common::LexicalCast<std::string>(value.GetValue());
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key,
                const SettableValue<std::string>& value)
            {
                if (value.IsSet())
                    map[key] = value.GetValue();
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key,
                const SettableValue<bool>& value)
            {
                if (value.IsSet())
                    map[key] = value.GetValue() ? "true" : "false";
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key,
                const SettableValue<ProtocolVersion>& value)
            {
                if (value.IsSet())
                    map[key] = value.GetValue().ToString();
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key,
                const SettableValue< std::vector<EndPoint> >& value)
            {
                if (value.IsSet())
                    map[key] = AddressesToString(value.GetValue());
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key,
                const SettableValue<ssl::SslMode::Type>& value)
            {
                if (value.IsSet())
                    map[key] = ssl::SslMode::ToString(value.GetValue());
            }

            template<>
            void Configuration::AddToMap(ArgumentMap& map, const std::string& key,
                const SettableValue<NestedTxMode::Type>& value)
            {
                if (value.IsSet())
                    map[key] = NestedTxMode::ToString(value.GetValue());
            }
        }
    }
}

