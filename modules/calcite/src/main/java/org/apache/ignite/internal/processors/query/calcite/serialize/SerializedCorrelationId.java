/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.io.Serializable;
import org.apache.calcite.rel.core.CorrelationId;

/**
 *
 */
public class SerializedCorrelationId implements Serializable {
    private final int id;
    private final String name;

    public SerializedCorrelationId(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public SerializedCorrelationId(CorrelationId corrId) {
        id = corrId.getId();
        name = corrId.getName();
    }

    public int id() {
        return id;
    }

    public String name() {
        return name;
    }
}
