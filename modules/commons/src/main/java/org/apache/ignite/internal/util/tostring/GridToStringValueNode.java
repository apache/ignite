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

package org.apache.ignite.internal.util.tostring;

import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Represents a simple value node
 * that appends its string representation to a builder.
 */
class GridToStringValueNode extends GridToStringNode {
    /** The string representation of the value. */
    private final String val;

    /**
     * Constructs a new value node.
     * @param propName Name of the property.
     * @param val The value to be stringified.
     */
    GridToStringValueNode(String propName, Object val) {
        super(propName);
        this.val = (val instanceof String) ? (String) val : String.valueOf(val);
    }

    /** {@inheritDoc} */
    @Override void appendNode(GridStringBuilder sb) {
        super.appendNode(sb);
        sb.a(val);
    }
}
