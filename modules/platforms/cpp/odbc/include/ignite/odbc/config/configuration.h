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

#include <ignite/common/common.h>
#include <ignite/common/utils.h>
#include "ignite/odbc/protocol_version.h"

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
                /** Map containing connect arguments. */
                typedef std::map<std::string, std::string> ArgumentMap;

                /** Connection attribute keywords. */
                struct Key
                {
                    /** Connection attribute keyword for DSN attribute. */
                    static const std::string dsn;

                    /** Connection attribute keyword for Driver attribute. */
                    static const std::string driver;

                    /** Connection attribute keyword for schema attribute. */
                    static const std::string schema;

                    /** Connection attribute keyword for address attribute. */
                    static const std::string address;

                    /** Connection attribute keyword for server attribute. */
                    static const std::string server;

                    /** Connection attribute keyword for port attribute. */
                    static const std::string port;

                    /** Connection attribute keyword for distributed joins attribute. */
                    static const std::string distributedJoins;

                    /** Connection attribute keyword for enforce join order attribute. */
                    static const std::string enforceJoinOrder;

                    /** Connection attribute keyword for protocol version attribute. */
                    static const std::string protocolVersion;

                    /** Connection attribute keyword for fetch results page size attribute. */
                    static const std::string pageSize;

                    /** Connection attribute keyword for replicated only attribute. */
                    static const std::string replicatedOnly;

                    /** Connection attribute keyword for collocated attribute. */
                    static const std::string collocated;

                    /** Connection attribute keyword for lazy attribute. */
                    static const std::string lazy;
                };

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
                };

                /**
                 * Connection end point structure.
                 */
                struct EndPoint
                {
                    /** Remote host. */
                    std::string host;

                    /** TCP port. */
                    uint16_t port;
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
                 * Fill configuration data using connection string.
                 *
                 * @param str Pointer to string data.
                 * @param len String length.
                 */
                void FillFromConnectString(const char* str, size_t len);
                
                /**
                 * Fill configuration data using connection string.
                 *
                 * @param str Connect string.
                 */
                void FillFromConnectString(const std::string& str)
                {
                    FillFromConnectString(str.data(), str.size());
                }

                /**
                 * Convert configure to connect string.
                 *
                 * @return Connect string.
                 */
                std::string ToConnectString() const;

                /**
                 * Fill configuration data using config attributes string.
                 *
                 * @param str Pointer to list of zero-terminated strings.
                 *            Terminated by two zero bytes.
                 */
                void FillFromConfigAttributes(const char* attributes);

                /**
                 * Get server port.
                 *
                 * @return Server port.
                 */
                uint16_t GetTcpPort() const
                {
                    return endPoint.port;
                }

                /**
                 * Set server port.
                 *
                 * @param port Server port.
                 */
                void SetTcpPort(uint16_t port);

                /**
                 * Get DSN.
                 *
                 * @return Data Source Name.
                 */
                const std::string& GetDsn(const std::string& dflt = DefaultValue::dsn) const
                {
                    return GetStringValue(Key::dsn, dflt);
                }

                /**
                 * Set DSN.
                 *
                 * @param dsn Data Source Name.
                 */
                void SetDsn(const std::string& dsn)
                {
                    arguments[Key::dsn] = dsn;
                }

                /**
                 * Get Driver.
                 *
                 * @return Driver name.
                 */
                const std::string& GetDriver() const
                {
                    return GetStringValue(Key::driver, DefaultValue::driver);
                }

                /**
                 * Get server host.
                 *
                 * @return Server host.
                 */
                const std::string& GetHost() const
                {
                    return endPoint.host;
                }

                /**
                 * Set server host.
                 *
                 * @param server Server host.
                 */
                void SetHost(const std::string& server);

                /**
                 * Get schema.
                 *
                 * @return Schema.
                 */
                const std::string& GetSchema() const
                {
                    return GetStringValue(Key::schema, DefaultValue::schema);
                }

                /**
                 * Set schema.
                 *
                 * @param schema Schema name.
                 */
                void SetSchema(const std::string& schema)
                {
                    arguments[Key::schema] = schema;
                }

                /**
                 * Get address.
                 *
                 * @return Address.
                 */
                const std::string& GetAddress() const
                {
                    return GetStringValue(Key::address, DefaultValue::address);
                }

                /**
                 * Set address.
                 *
                 * @param address Address.
                 */
                void SetAddress(const std::string& address);

                /**
                 * Check distributed joins flag.
                 *
                 * @return True if distributed joins are enabled.
                 */
                bool IsDistributedJoins() const
                {
                    return GetBoolValue(Key::distributedJoins, DefaultValue::distributedJoins);
                }

                /**
                 * Set distributed joins.
                 *
                 * @param val Value to set.
                 */
                void SetDistributedJoins(bool val)
                {
                    SetBoolValue(Key::distributedJoins, val);
                }

                /**
                 * Check enforce join order flag.
                 *
                 * @return True if enforcing of join order is enabled.
                 */
                bool IsEnforceJoinOrder() const
                {
                    return GetBoolValue(Key::enforceJoinOrder, DefaultValue::enforceJoinOrder);
                }

                /**
                 * Set enforce joins.
                 *
                 * @param val Value to set.
                 */
                void SetEnforceJoinOrder(bool val)
                {
                    SetBoolValue(Key::enforceJoinOrder, val);
                }

                /**
                 * Check replicated only flag.
                 *
                 * @return True if replicated only is enabled.
                 */
                bool IsReplicatedOnly() const
                {
                    return GetBoolValue(Key::replicatedOnly, DefaultValue::replicatedOnly);
                }

                /**
                 * Set replicated only flag.
                 *
                 * @param val Value to set.
                 */
                void SetReplicatedOnly(bool val)
                {
                    SetBoolValue(Key::replicatedOnly, val);
                }

                /**
                 * Check collocated flag.
                 *
                 * @return True if collocated is enabled.
                 */
                bool IsCollocated() const
                {
                    return GetBoolValue(Key::collocated, DefaultValue::collocated);
                }

                /**
                 * Set collocated.
                 *
                 * @param val Value to set.
                 */
                void SetCollocated(bool val)
                {
                    SetBoolValue(Key::collocated, val);
                }

                /**
                 * Check lazy flag.
                 *
                 * @return True if lazy is enabled.
                 */
                bool IsLazy() const
                {
                    return GetBoolValue(Key::lazy, DefaultValue::lazy);
                }

                /**
                 * Set lazy.
                 *
                 * @param val Value to set.
                 */
                void SetLazy(bool val)
                {
                    SetBoolValue(Key::lazy, val);
                }

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
                void SetProtocolVersion(const std::string& version)
                {
                    arguments[Key::protocolVersion] = version;
                }

                /**
                 * Get argument map.
                 *
                 * @return Argument map.
                 */
                const ArgumentMap& GetMap() const
                {
                    return arguments;
                }

                /**
                 * Get fetch results page size.
                 *
                 * @return Fetch results page size.
                 */
                int32_t GetPageSize() const
                {
                    return static_cast<int32_t>(GetIntValue(Key::pageSize, DefaultValue::pageSize));
                }
                /**
                 * Set fetch results page size.
                 *
                 * @param size Fetch results page size.
                 */
                void SetPageSize(int32_t size)
                {
                    arguments[Key::pageSize] = common::LexicalCast<std::string>(size);
                }

                /**
                 * Get string value from the config.
                 *
                 * @param key Configuration key.
                 * @param dflt Default value to be returned if there is no value stored.
                 * @return Found or default value.
                 */
                const std::string& GetStringValue(const std::string& key, const std::string& dflt) const;

                /**
                 * Get int value from the config.
                 *
                 * @param key Configuration key.
                 * @param dflt Default value to be returned if there is no value stored.
                 * @return Found or default value.
                 */
                int64_t GetIntValue(const std::string& key, int64_t dflt) const;

                /**
                 * Get bool value from the config.
                 *
                 * @param key Configuration key.
                 * @param dflt Default value to be returned if there is no value stored.
                 * @return Found or default value.
                 */
                bool GetBoolValue(const std::string& key, bool dflt) const;

                /**
                 * Set bool value to the config.
                 *
                 * @param key Configuration key.
                 * @param val Value to set.
                 */
                void SetBoolValue(const std::string& key, bool val);
            private:
                /**
                 * Parse connect string into key-value storage.
                 *
                 * @param str String to parse.
                 * @param len String length.
                 * @param params Parsing result.
                 */
                static void ParseAttributeList(const char* str, size_t len, char delimeter, ArgumentMap& args);

                /**
                 * Parse address and extract connection end-point.
                 *
                 * @throw IgniteException if address can not be parsed.
                 * @param address Address string to parse.
                 * @param res Result is placed here.
                 */
                static void ParseAddress(const std::string& address, EndPoint& res);

                /** Arguments. */
                ArgumentMap arguments;

                /** Connection end-point. */
                EndPoint endPoint;
            };
        }

    }
}

#endif //_IGNITE_ODBC_CONFIG_CONFIGURATION
