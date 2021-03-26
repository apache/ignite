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

package org.apache.ignite.console.json;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A simple representation of a JSON array.
 */
public class JsonArray extends ArrayList<Object> {
    /**
     * Default constructor.
     */
    public JsonArray() {
        // No-op.
    }

    /**
     * Wrapping constructor.
     *
     * @param list List with items.
     */
    public JsonArray(Collection list) {
        super(list);
    }

    /**
     * @param idx Index of the element to return.
     * @return Element as {@link String}.
     */
    public String getString(int idx) {
        return (String)get(idx);
    }
}
