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

package org.apache.ignite.internal.managers.encryption;

import java.io.Serializable;

/**
 * Cache group encryption key with identifier.
 */
public class GroupKey {
    /** Encryption key ID. */
    private final int id;

    /** Encryption key. */
    private final Serializable key;

    /**
     * @param key Encryption key.
     * @param id Encryption key ID.
     */
    public GroupKey(Serializable key, int id) {
        this.id = id;
        this.key = key;
    }

    /**
     * @return Encryption key ID.
     */
    public byte id() {
        return (byte)id;
    }

    /**
     * @return Encryption key.
     */
    public Serializable key() {
        return key;
    }
}
