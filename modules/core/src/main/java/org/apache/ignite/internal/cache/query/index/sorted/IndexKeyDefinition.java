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

package org.apache.ignite.internal.cache.query.index.sorted;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.cache.query.index.IndexKeyTypeMessage;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Defines a signle index key.
 */
public class IndexKeyDefinition implements Message {
    /** A message for {@link IndexKeyType}. */
    @Order(0)
    IndexKeyTypeMessage idxTypeMsg;

    /** Order. */
    @Order(1)
    boolean asc;

    /** Precision for variable length key types. */
    @Order(2)
    int precision;

    /** */
    public IndexKeyDefinition() {
        // No-op.
    }

    /** */
    public IndexKeyDefinition(int idxTypeCode, long precision, boolean asc) {
        idxTypeMsg = new IndexKeyTypeMessage(idxTypeCode);

        this.asc = asc;

        // Workaround due to wrong type conversion (int -> long).
        if (precision >= Integer.MAX_VALUE)
            this.precision = -1;
        else
            this.precision = (int)precision;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 113;
    }

    /** */
    public boolean ascending() {
        return asc;
    }

    /** */
    public IndexKeyType indexKeyType() {
        return idxTypeMsg.value();
    }

    /** */
    public int precision() {
        return precision;
    }

}
