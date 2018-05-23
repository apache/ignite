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

#ifndef _IGNITE_THIN_IGNITE_CLIENT_CONFIGURATION
#define _IGNITE_THIN_IGNITE_CLIENT_CONFIGURATION

#include <string>

#include <ignite/thin/ssl_mode.h>

namespace ignite
{
    namespace thin
    {
        class IgniteClientConfiguration
        {
        public:
            IgniteClientConfiguration() :
                sslMode(SslMode::DISABLE)
            {
                // No-op.
            }

            const std::string& GetEndPoints() const
            {
                return endPoints;
            }

            void SetEndPoints(const std::string& endPoints)
            {
                this->endPoints = endPoints;
            }

            const std::string& GetUser() const
            {
                return user;
            }

            void SetUser(const std::string& user)
            {
                this->user = user;
            }

            const std::string& GetPassword() const
            {
                return password;
            }

            void SetPassword(const std::string& password)
            {
                this->password = password;
            }

            SslMode::Type GetSslMode() const
            {
                return sslMode;
            }

            void SetSslMode(SslMode::Type sslMode)
            {
                this->sslMode = sslMode;
            }

            const std::string& GetSslCertFile() const
            {
                return sslCertFile;
            }

            void SetSslCertFile(const std::string& sslCertFile)
            {
                this->sslCertFile = sslCertFile;
            }

            const std::string& GetSslKeyFile() const
            {
                return sslKeyFile;
            }

            void SetSslKeyFile(const std::string& sslKeyFile)
            {
                this->sslKeyFile = sslKeyFile;
            }

            const std::string& GetSslCaFile() const
            {
                return sslCaFile;
            }

            void SetSslCaFile(const std::string& sslCaFile)
            {
                this->sslCaFile = sslCaFile;
            }

        private:
            /** Connection end points */
            std::string endPoints;

            /** Username. */
            std::string user;

            /** Password. */
            std::string password;

            /** SSL mode */
            SslMode::Type sslMode;

            /** SSL client certificate path */
            std::string sslCertFile;

            /** SSL client key path */
            std::string sslKeyFile;

            /** SSL client certificate authority path */            
            std::string sslCaFile;
        };
    }
}
#endif // _IGNITE_THIN_IGNITE_CLIENT_CONFIGURATION
