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

#ifndef _IGNITE_EXAMPLES_ACCOUNT
#define _IGNITE_EXAMPLES_ACCOUNT

#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "ignite/examples/client.h"

namespace ignite
{
    namespace examples
    {
        struct Account
        {
            Account() : accountId(0), clientId(0), balance(0.0)
            {
                // No-op.
            }

            Account(int64_t accountId, int64_t clientId, double balance) :
                accountId(accountId),
                clientId(clientId),
                balance(balance)
            {
                // No-op.
            }

            Account(int64_t accountId, const Client& client, double balance) :
                accountId(accountId),
                clientId(client.clientId),
                balance(balance)
            {
                // No-op.
            }

            int64_t accountId;
            int64_t clientId;
            double balance;
        };
    }
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::examples::Account)

            typedef ignite::examples::Account Account;

            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(Account)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(Account)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(Account)
            IGNITE_BINARY_IS_NULL_FALSE(Account)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(Account)

            void Write(BinaryWriter& writer, Account obj)
            {
                writer.WriteInt64("accountId", obj.accountId);
                writer.WriteInt64("clientId", obj.clientId);
                writer.WriteDouble("balance", obj.balance);
            }

            Account Read(BinaryReader& reader)
            {
                int64_t accountId = reader.ReadInt64("accountId");
                int64_t clientId = reader.ReadInt64("clientId");
                double balance = reader.ReadDouble("balance");

                return Account(accountId, clientId, balance);
            }

        IGNITE_BINARY_TYPE_END
    }
};

#endif // _IGNITE_EXAMPLES_ACCOUNT