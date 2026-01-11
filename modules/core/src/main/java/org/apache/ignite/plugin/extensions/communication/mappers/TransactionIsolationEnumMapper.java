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

package org.apache.ignite.plugin.extensions.communication.mappers;

import org.apache.ignite.transactions.TransactionIsolation;

/** */
public class TransactionIsolationEnumMapper implements EnumMapper<TransactionIsolation> {
    /** {@inheritDoc} */
    @Override public byte encode(TransactionIsolation val) {
        if (val == null)
            return -1;

        switch (val) {
            case READ_COMMITTED:
                return 0;
            case REPEATABLE_READ:
                return 1;
            case SERIALIZABLE:
                return 2;
            default:
                return -1;
        }
    }

    /** {@inheritDoc} */
    @Override public TransactionIsolation decode(byte code) {
        if (code == -1)
            return null;

        switch (code) {
            case 0:
                return TransactionIsolation.READ_COMMITTED;
            case 1:
                return TransactionIsolation.REPEATABLE_READ;
            case 2:
                return TransactionIsolation.SERIALIZABLE;
            default:
                return null;
        }
    }
}
