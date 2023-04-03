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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Filtrable;
import org.apache.ignite.internal.managers.systemview.walker.Order;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/**
 * Baseline node attribute representation for a {@link SystemView}.
 */
public class BaselineNodeAttributeView {
    /** Node consistent id. */
    private final Object consistentId;

    /** Attribute name. */
    private final String name;

    /** Attribute value. */
    private final Object val;

    /**
     * @param consistentId Node consistent id.
     * @param name Attribute name.
     * @param val Attribute value.
     */
    public BaselineNodeAttributeView(Object consistentId, String name, Object val) {
        this.consistentId = consistentId;
        this.name = name;
        this.val = val;
    }

    /**
     * @return Node consistent id.
     */
    @Order
    @Filtrable
    public String nodeConsistentId() {
        return toStringSafe(consistentId);
    }

    /**
     * @return Attribute name.
     */
    @Order(1)
    @Filtrable
    public String name() {
        return name;
    }

    /**
     * @return Attribute value.
     */
    @Order(2)
    public String value() {
        return toStringSafe(val);
    }
}
