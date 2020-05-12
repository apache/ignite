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

package org.apache.ignite.internal.binary;

import java.io.Serializable;

/**
 * Binary object array.
 */
public final class BinaryObjectArray implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Item type id. */
    private final int itemTypeId;

    /** Items. */
    private final Object[] items;

    /**
     * @param itemTypeId Item type id.
     * @param items Items.
     */
    BinaryObjectArray(int itemTypeId, Object[] items) {
        this.itemTypeId = itemTypeId;
        this.items = items;
    }

    /**
     * @return item type id.
     */
    public int itemTypeId() {
        return itemTypeId;
    }

    /**
     * @return items.
     */
    public Object[] items() {
        return items;
    }
}
