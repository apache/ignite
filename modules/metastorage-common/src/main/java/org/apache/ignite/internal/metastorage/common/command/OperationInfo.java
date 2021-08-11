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

package org.apache.ignite.internal.metastorage.common.command;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.common.OperationType;

/**
 * Defines operation.
 */
public class OperationInfo implements Serializable {
    /** Key. */
    private final byte[] key;

    /** Value. */
    private final byte[] val;

    /** Operation type. */
    private final OperationType type;

    /**
     * Constructs operation with given parameters.
     *
     * @param key Key.
     * @param val Value.
     * @param type Operation type.
     */
    public OperationInfo(byte[] key, byte[] val, OperationType type) {
        this.key = key;
        this.val = val;
        this.type = type;
    }

    /**
     * Returns operation type.
     *
     * @return Operation type.
     */
    public OperationType type() {
        return type;
    }

    /**
     * Returns key.
     *
     * @return Key.
     */
    public byte[] key() {
        return key;
    }

    /**
     * Returns value.
     *
     * @return Value.
     */
    public byte[] value() {
        return val;
    }
}
