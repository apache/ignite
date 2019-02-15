/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

/**
 * @file
 * Declares ignite::thin::IgniteClientConfiguration class.
 */

#ifndef _IGNITE_THIN_IGNITE_CLIENT_CONFIGURATION
#define _IGNITE_THIN_IGNITE_CLIENT_CONFIGURATION

#include <string>

#include <ignite/thin/ssl_mode.h>

namespace ignite
{
    namespace thin
    {
        /**
         * Ignite thin client configuration.
         *
         * Used to configure IgniteClient.
         */
        class IgniteClientConfiguration
        {
        public:
            /**
             * Default constructor.
             *
             * Constructs configuration with all parameters set to default values.
             */
            IgniteClientConfiguration() :
                sslMode(SslMode::DISABLE)
            {
                // No-op.
            }

            /**
             * Get server end points.
             * @see SetEndPoints for format.
             * @return Server end points.
             */
            const std::string& GetEndPoints() const
            {
                return endPoints;
            }

            /**
             * Set addressess of the remote servers to connect.
             *
             * The format of the addresse is: <host>[:<port>[..<port_range>]]. If port is not specified, default port
             * is used (10800). You can enlist several hosts separated by comma.
             *
             * For example: "localhost,example.com:12345,127.0.0.1:10800..10900,192.168.3.80:5893".
             *
             *  @param endPoints Addressess of the remote servers to connect.
             */
            void SetEndPoints(const std::string& endPoints)
            {
                this->endPoints = endPoints;
            }

            /**
             * Get user name used for the authentication.
             *
             * @return User name.
             */
            const std::string& GetUser() const
            {
                return user;
            }

            /**
             * Set user name to use for the authentication.
             *
             * @param user User name.
             */
            void SetUser(const std::string& user)
            {
                this->user = user;
            }

            /**
             * Get password used for the authentication.
             *
             * @return Password.
             */
            const std::string& GetPassword() const
            {
                return password;
            }

            /**
             * Set password to use for the authentication.
             *
             * @param password Password.
             */
            void SetPassword(const std::string& password)
            {
                this->password = password;
            }

            /**
             * Get SSL mode.
             *
             * @see SslMode for details.
             *
             * @return SSL mode.
             */
            SslMode::Type GetSslMode() const
            {
                return sslMode;
            }

            /**
             * Set SSL mode.
             *
             * @see SslMode for details.
             *
             * @param sslMode SSL mode.
             */
            void SetSslMode(SslMode::Type sslMode)
            {
                this->sslMode = sslMode;
            }

            /**
             * Get file path to SSL certificate to use during connection establishment.
             *
             * @return File path to SSL certificate.
             */
            const std::string& GetSslCertFile() const
            {
                return sslCertFile;
            }

            /**
             * Set file path to SSL certificate to use during connection establishment.
             *
             * @param sslCertFile File path to SSL certificate.
             */
            void SetSslCertFile(const std::string& sslCertFile)
            {
                this->sslCertFile = sslCertFile;
            }

            /**
             * Get file path to SSL private key to use during connection establishment.
             *
             * @return File path to SSL private key.
             */
            const std::string& GetSslKeyFile() const
            {
                return sslKeyFile;
            }

            /**
             * Set file path to SSL private key to use during connection establishment.
             *
             * @param sslKeyFile File path to SSL private key.
             */
            void SetSslKeyFile(const std::string& sslKeyFile)
            {
                this->sslKeyFile = sslKeyFile;
            }

            /**
             * Get file path to SSL certificate authority to authenticate server certificate during connection
             *  establishment.
             *
             * @return File path to SSL certificate authority.
             */
            const std::string& GetSslCaFile() const
            {
                return sslCaFile;
            }

            /**
             * Set file path to SSL certificate authority to authenticate server certificate during connection
             *  establishment.
             *
             * @param sslCaFile File path to SSL certificate authority.
             */
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
