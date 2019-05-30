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

#ifndef _IGNITE_ODBC_CONFIG_CONFIGURATION
#define _IGNITE_ODBC_CONFIG_CONFIGURATION

#include <stdint.h>
#include <string>
#include <map>

#include "ignite/odbc/protocol_version.h"
#include "ignite/odbc/config/settable_value.h"
#include "ignite/odbc/ssl_mode.h"
#include "ignite/odbc/end_point.h"
#include "ignite/odbc/nested_tx_mode.h"

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            /**
             * ODBC configuration abstraction.
             */
            class Configuration
            {
            public:
                /** Argument map type. */
                typedef std::map<std::string, std::string> ArgumentMap;

                /** Default values for configuration. */
                struct DefaultValue
                {
                    /** Default value for DSN attribute. */
                    static const std::string dsn;

                    /** Default value for Driver attribute. */
                    static const std::string driver;

                    /** Default value for schema attribute. */
                    static const std::string schema;

                    /** Default value for address attribute. */
                    static const std::string address;

                    /** Default value for server attribute. */
                    static const std::string server;

                    /** Default value for sslMode attribute. */
                    static const ssl::SslMode::Type sslMode;

                    /** Default value for sslKeyFile attribute. */
                    static const std::string sslKeyFile;

                    /** Default value for sslCertFile attribute. */
                    static const std::string sslCertFile;

                    /** Default value for sslCaFile attribute. */
                    static const std::string sslCaFile;

                    /** Default value for protocol version. */
                    static const ProtocolVersion& protocolVersion;

                    /** Default value for port attribute. */
                    static const uint16_t port;

                    /** Default value for fetch results page size attribute. */
                    static const int32_t pageSize;

                    /** Default value for distributed joins attribute. */
                    static const bool distributedJoins;

                    /** Default value for enforce join order attribute. */
                    static const bool enforceJoinOrder;

                    /** Default value for replicated only attribute. */
                    static const bool replicatedOnly;

                    /** Default value for collocated attribute. */
                    static const bool collocated;

                    /** Default value for lazy attribute. */
                    static const bool lazy;

                    /** Default value for skipReducerOnUpdate attribute. */
                    static const bool skipReducerOnUpdate;

                    /** Default value for user attribute. */
                    static const std::string user;

                    /** Default value for password attribute. */
                    static const std::string password;

                    /** Default value for nestedTxMode attribute. */
                    static const NestedTxMode::Type nestedTxMode;
                };

                /**
                 * Default constructor.
                 */
                Configuration();

                /**
                 * Destructor.
                 */
                ~Configuration();

                /**
                 * Convert configure to connect string.
                 *
                 * @return Connect string.
                 */
                std::string ToConnectString() const;

                /**
                 * Get server port.
                 *
                 * @return Server port.
                 */
                uint16_t GetTcpPort() const;

                /**
                 * Set server port.
                 *
                 * @param port Server port.
                 */
                void SetTcpPort(uint16_t port);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsTcpPortSet() const;

                /**
                 * Get DSN.
                 *
                 * @return Data Source Name.
                 */
                const std::string& GetDsn(const std::string& dflt = DefaultValue::dsn) const;

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsDsnSet() const;

                /**
                 * Set DSN.
                 *
                 * @param dsn Data Source Name.
                 */
                void SetDsn(const std::string& dsn);

                /**
                 * Get Driver.
                 *
                 * @return Driver name.
                 */
                const std::string& GetDriver() const;

                /**
                 * Set driver.
                 *
                 * @param driver Driver.
                 */
                void SetDriver(const std::string& driver);

                /**
                 * Get server host.
                 *
                 * @return Server host.
                 */
                const std::string& GetHost() const;

                /**
                 * Set server host.
                 *
                 * @param server Server host.
                 */
                void SetHost(const std::string& server);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsHostSet() const;

                /**
                 * Get schema.
                 *
                 * @return Schema.
                 */
                const std::string& GetSchema() const;

                /**
                 * Set schema.
                 *
                 * @param schema Schema name.
                 */
                void SetSchema(const std::string& schema);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsSchemaSet() const;

                /**
                 * Get addresses.
                 *
                 * @return Addresses.
                 */
                const std::vector<EndPoint>& GetAddresses() const;

                /**
                 * Set addresses to connect to.
                 *
                 * @param endPoints Addresses.
                 */
                void SetAddresses(const std::vector<EndPoint>& endPoints);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsAddressesSet() const;

                /**
                 * Get SSL mode.
                 *
                 * @return SSL mode.
                 */
                ssl::SslMode::Type GetSslMode() const;

                /**
                 * Set SSL mode.
                 *
                 * @param sslMode SSL mode.
                 */
                void SetSslMode(ssl::SslMode::Type sslMode);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsSslModeSet() const;

                /**
                 * Get SSL key file path.
                 *
                 * @return SSL key file path.
                 */
                const std::string& GetSslKeyFile() const;

                /**
                 * Set SSL key file path.
                 *
                 * @param sslKeyFile SSL key file path.
                 */
                void SetSslKeyFile(const std::string& sslKeyFile);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsSslKeyFileSet() const;

                /**
                 * Get SSL certificate file path.
                 *
                 * @return SSL certificate file path.
                 */
                const std::string& GetSslCertFile() const;

                /**
                 * Set SSL certificate file path.
                 *
                 * @param sslCertFile SSL certificate file path.
                 */
                void SetSslCertFile(const std::string& sslCertFile);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsSslCertFileSet() const;

                /**
                 * Get SSL certificate authority file path.
                 *
                 * @return SSL certificate authority file path.
                 */
                const std::string& GetSslCaFile() const;

                /**
                 * Set SSL certificate authority file path.
                 *
                 * @param sslCaFile SSL certificate authority file path.
                 */
                void SetSslCaFile(const std::string& sslCaFile);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsSslCaFileSet() const;

                /**
                 * Check distributed joins flag.
                 *
                 * @return True if distributed joins are enabled.
                 */
                bool IsDistributedJoins() const;

                /**
                 * Set distributed joins.
                 *
                 * @param val Value to set.
                 */
                void SetDistributedJoins(bool val);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsDistributedJoinsSet() const;

                /**
                 * Check enforce join order flag.
                 *
                 * @return True if enforcing of join order is enabled.
                 */
                bool IsEnforceJoinOrder() const;

                /**
                 * Set enforce joins.
                 *
                 * @param val Value to set.
                 */
                void SetEnforceJoinOrder(bool val);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsEnforceJoinOrderSet() const;

                /**
                 * Check replicated only flag.
                 *
                 * @return True if replicated only is enabled.
                 */
                bool IsReplicatedOnly() const;

                /**
                 * Set replicated only flag.
                 *
                 * @param val Value to set.
                 */
                void SetReplicatedOnly(bool val);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsReplicatedOnlySet() const;

                /**
                 * Check collocated flag.
                 *
                 * @return True if collocated is enabled.
                 */
                bool IsCollocated() const;

                /**
                 * Set collocated.
                 *
                 * @param val Value to set.
                 */
                void SetCollocated(bool val);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsCollocatedSet() const;

                /**
                 * Check lazy flag.
                 *
                 * @return True if lazy is enabled.
                 */
                bool IsLazy() const;

                /**
                 * Set lazy.
                 *
                 * @param val Value to set.
                 */
                void SetLazy(bool val);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsLazySet() const;

                /**
                 * Check update on server flag.
                 *
                 * @return True if update on server.
                 */
                bool IsSkipReducerOnUpdate() const;

                /**
                 * Set update on server.
                 *
                 * @param val Value to set.
                 */
                void SetSkipReducerOnUpdate(bool val);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsSkipReducerOnUpdateSet() const;

                /**
                 * Get protocol version.
                 *
                 * @return Protocol version.
                 */
                ProtocolVersion GetProtocolVersion() const;

                /**
                 * Set protocol version.
                 *
                 * @param version Version to set.
                 */
                void SetProtocolVersion(const ProtocolVersion& version);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsProtocolVersionSet() const;

                /**
                 * Get fetch results page size.
                 *
                 * @return Fetch results page size.
                 */
                int32_t GetPageSize() const;

                /**
                 * Set fetch results page size.
                 *
                 * @param size Fetch results page size.
                 */
                void SetPageSize(int32_t size);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsPageSizeSet() const;

                /**
                 * Get user.
                 *
                 * @return User.
                 */
                const std::string& GetUser() const;

                /**
                 * Set user.
                 *
                 * @param user User.
                 */
                void SetUser(const std::string& user);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsUserSet() const;

                /**
                 * Get password.
                 *
                 * @return Password.
                 */
                const std::string& GetPassword() const;

                /**
                 * Set password.
                 *
                 * @param pass Password.
                 */
                void SetPassword(const std::string& pass);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsPasswordSet() const;

                /**
                 * Get nested transaction mode.
                 *
                 * @return Nested transaction mode.
                 */
                NestedTxMode::Type GetNestedTxMode() const;

                /**
                 * Set nested transaction mode.
                 *
                 * @param mode Nested transaction mode.
                 */
                void SetNestedTxMode(NestedTxMode::Type mode);

                /**
                 * Check if the value set.
                 *
                 * @return @true if the value set.
                 */
                bool IsNestedTxModeSet() const;

                /**
                 * Get argument map.
                 *
                 * @param res Resulting argument map.
                 */
                void ToMap(ArgumentMap& res) const;

            private:
                /**
                 * Add key and value to the argument map.
                 *
                 * @param map Map.
                 * @param key Key.
                 * @param value Value.
                 */
                template<typename T>
                static void AddToMap(ArgumentMap& map, const std::string& key, const SettableValue<T>& value);

                /** DSN. */
                SettableValue<std::string> dsn;

                /** Driver name. */
                SettableValue<std::string> driver;

                /** Schema. */
                SettableValue<std::string> schema;

                /** Server. Deprecated. */
                SettableValue<std::string> server;

                /** TCP port. Deprecated. */
                SettableValue<uint16_t> port;

                /** Request and response page size. */
                SettableValue<int32_t> pageSize;

                /** Distributed joins flag. */
                SettableValue<bool> distributedJoins;

                /** Enforce join order flag. */
                SettableValue<bool> enforceJoinOrder;

                /** Replicated only flag. */
                SettableValue<bool> replicatedOnly;

                /** Collocated flag. */
                SettableValue<bool> collocated;

                /** Lazy flag. */
                SettableValue<bool> lazy;

                /** Skip reducer on update flag. */
                SettableValue<bool> skipReducerOnUpdate;

                /** Protocol version. */
                SettableValue<ProtocolVersion> protocolVersion;

                /** Connection end-points. */
                SettableValue< std::vector<EndPoint> > endPoints;

                /** SSL Mode. */
                SettableValue<ssl::SslMode::Type> sslMode;

                /** SSL private key file path. */
                SettableValue<std::string> sslKeyFile;

                /** SSL certificate file path. */
                SettableValue<std::string> sslCertFile;

                /** SSL certificate authority file path. */
                SettableValue<std::string> sslCaFile;

                /** User. */
                SettableValue<std::string> user;

                /** Password. */
                SettableValue<std::string> password;

                /** Nested transaction mode. */
                SettableValue<NestedTxMode::Type> nestedTxMode;
            };

            template<>
            void Configuration::AddToMap<std::string>(ArgumentMap& map, const std::string& key,
                const SettableValue<std::string>& value);

            template<>
            void Configuration::AddToMap<uint16_t>(ArgumentMap& map, const std::string& key,
                const SettableValue<uint16_t>& value);

            template<>
            void Configuration::AddToMap<int32_t>(ArgumentMap& map, const std::string& key,
                const SettableValue<int32_t>& value);

            template<>
            void Configuration::AddToMap<bool>(ArgumentMap& map, const std::string& key,
                const SettableValue<bool>& value);

            template<>
            void Configuration::AddToMap<ProtocolVersion>(ArgumentMap& map, const std::string& key,
                const SettableValue<ProtocolVersion>& value);

            template<>
            void Configuration::AddToMap< std::vector<EndPoint> >(ArgumentMap& map, const std::string& key,
                const SettableValue< std::vector<EndPoint> >& value);

            template<>
            void Configuration::AddToMap<ssl::SslMode::Type>(ArgumentMap& map, const std::string& key,
                const SettableValue<ssl::SslMode::Type>& value);

            template<>
            void Configuration::AddToMap<NestedTxMode::Type>(ArgumentMap& map, const std::string& key,
                const SettableValue<NestedTxMode::Type>& value);
        }
    }
}

#endif //_IGNITE_ODBC_CONFIG_CONFIGURATION
