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

import org.apache.ignite.cache.query.index.sorted.Order;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;

public class IndexKeyDefinition {

    // TODO: Nullable, as may skip inlining due to previous keys.
    private InlineIndexKeyType inlineType;

    private final int idxType;

    private final Order order;

    public IndexKeyDefinition(int idxType, boolean inline) {
        this(idxType, inline, Order.DEFAULT);
    }

    public IndexKeyDefinition(int idxType, boolean inline, Order order) {
        this.idxType = idxType;
        this.order = order;

        if (inline) {
            assert InlineIndexKeyTypeRegistry.supportInline(idxType): idxType;

            inlineType = InlineIndexKeyTypeRegistry.get(idxType);
        }
    }

    public InlineIndexKeyType getInlineType() {
        return inlineType;
    }

    public Order getOrder() {
        return order;
    }

    public int getIdxType() {
        return idxType;
    }
}
