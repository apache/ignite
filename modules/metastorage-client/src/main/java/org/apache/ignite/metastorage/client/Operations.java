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

package org.apache.ignite.metastorage.client;

import org.apache.ignite.lang.ByteArray;

/**
 * This class contains fabric methods which produce operations needed for conditional multi update functionality
 * (invoke) provided by meta storage service.
 *
 * @see Operation
 */
public final class Operations {
    /** No-op operation singleton. */
    private static final Operation.NoOp NO_OP = new Operation.NoOp();

    /**
     * Creates operation of type <i>remove</i>. This type of operation removes entry.
     *
     * @param key Identifies an entry which operation will be applied to.
     * @return Operation of type <i>remove</i>.
     */
    public static Operation remove(ByteArray key) {
        return new Operation(new Operation.RemoveOp(key.bytes()));
    }

    /**
     * Creates operation of type <i>put</i>. This type of operation inserts or updates value of entry.
     *
     * @param key Identifies an entry which operation will be applied to.
     * @param value Value.
     * @return Operation of type <i>put</i>.
     */
    public static Operation put(ByteArray key, byte[] value) {
        return new Operation(new Operation.PutOp(key.bytes(), value));
    }

    /**
     * Creates operation of type <i>noop</i>. It is a special type of operation which doesn't perform any action.
     *
     * @return Operation of type <i>noop</i>.
     */
    public static Operation noop() {
        return new Operation(NO_OP);
    }

    /**
     * Default no-op constructor.
     */
    private Operations() {
        // No-op.
    }
}
