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

package org.apache.ignite.internal.thread.context;

import java.lang.reflect.Field;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.thread.context.OperationContextDispatcher.MAX_ATTRS_CNT;

/**
 * Declares every distributed {@link OperationContext} attribute ID this release knows, along with the feature that
 * introduced each of them.
 *
 * <p></p>Distributed Attribute key is used to consistently identify {@link OperationContext} attributes across
 * all nodes in the cluster.</p>
 *
 * @see DistributedAttributeKey
 * @see OperationContextDispatcher
 */
public class DistributedAttributeKeyRegistry {
    /** Attribute reserved for {@link SecurityContext} propagation. */
    public static final DistributedAttributeKey SECURITY = new DistributedAttributeKey(0);

    /** Package private so that tests can declare keys that are not constants of this registry. */
    static final DistributedAttributeKey[] VALS = new DistributedAttributeKey[MAX_ATTRS_CNT];

    static {
        try {
            for (Field field : DistributedAttributeKeyRegistry.class.getFields()) {
                DistributedAttributeKey key = (DistributedAttributeKey)field.get(null);

                assert VALS[key.id()] == null : "Duplicated distributed attribute id [id=" + key.id() + ']';

                VALS[key.id()] = key;
            }
        }
        catch (IllegalAccessException e) {
            throw new IgniteException("Failed to read Distributed Attribute Key Registry", e);
        }
    }

    /** */
    public static @Nullable DistributedAttributeKey get(int id) {
        return id >= 0 && id < VALS.length ? VALS[id] : null;
    }
}
