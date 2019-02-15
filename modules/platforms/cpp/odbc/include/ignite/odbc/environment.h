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

#ifndef _IGNITE_ODBC_ENVIRONMENT
#define _IGNITE_ODBC_ENVIRONMENT

#include <set>

#include "ignite/odbc/diagnostic/diagnosable_adapter.h"

namespace ignite
{
    namespace odbc
    {
        class Connection;

        /**
         * ODBC environment.
         */
        class Environment : public diagnostic::DiagnosableAdapter
        {
        public:
            /** Connection set type. */
            typedef std::set<Connection*> ConnectionSet;

            /**
             * Constructor.
             */
            Environment();

            /**
             * Destructor.
             */
            ~Environment();

            /**
             * Create connection associated with the environment.
             *
             * @return Pointer to valid instance on success or NULL on failure.
             */
            Connection* CreateConnection();

            /**
             * Deregister connection.
             *
             * @param conn Connection to deregister.
             */
            void DeregisterConnection(Connection* conn);

            /**
             * Perform transaction commit on all the associated connections.
             */
            void TransactionCommit();

            /**
             * Perform transaction rollback on all the associated connections.
             */
            void TransactionRollback();

            /**
             * Set attribute.
             *
             * @param attr Attribute to set.
             * @param value Value.
             * @param len Value length if the attribute is of string type.
             */
            void SetAttribute(int32_t attr, void* value, int32_t len);

            /**
             * Get attribute.
             *
             * @param attr Attribute to set.
             * @param buffer Buffer to put value to.
             */
            void GetAttribute(int32_t attr, app::ApplicationDataBuffer& buffer);

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Environment);

            /**
             * Create connection associated with the environment.
             * Internal call.
             *
             * @return Pointer to valid instance on success or NULL on failure.
             * @return Operation result.
             */
            SqlResult::Type InternalCreateConnection(Connection*& connection);

            /**
             * Perform transaction commit on all the associated connections.
             * Internal call.
             *
             * @return Operation result.
             */
            SqlResult::Type InternalTransactionCommit();

            /**
             * Perform transaction rollback on all the associated connections.
             * Internal call.
             *
             * @return Operation result.
             */
            SqlResult::Type InternalTransactionRollback();

            /**
             * Set attribute.
             * Internal call.
             *
             * @param attr Attribute to set.
             * @param value Value.
             * @param len Value length if the attribute is of string type.
             * @return Operation result.
             */
            SqlResult::Type InternalSetAttribute(int32_t attr, void* value, int32_t len);

            /**
             * Get attribute.
             * Internal call.
             *
             * @param attr Attribute to set.
             * @param buffer Buffer to put value to.
             * @return Operation result.
             */
            SqlResult::Type InternalGetAttribute(int32_t attr, app::ApplicationDataBuffer& buffer);

            /** Assotiated connections. */
            ConnectionSet connections;

            /** ODBC version. */
            int32_t odbcVersion;

            /** ODBC null-termintaion of string behaviour. */
            int32_t odbcNts;
        };
    }
}

#endif //_IGNITE_ODBC_ENVIRONMENT