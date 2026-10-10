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

package org.apache.ignite.plugin.extensions.communication;

import java.util.Arrays;

/** Serialized value of a field this node cannot decode, forwarded to the next peer as is. */
public final class RawField {
    /** */
    private final int tag;

    /** */
    private final byte[] bytes;

    /**
     * @param tag Tag of the field: the id of the feature that gates it.
     * @param bytes Serialized field value.
     */
    public RawField(int tag, byte[] bytes) {
        this.tag = tag;
        this.bytes = bytes;
    }

    /** @return Tag of the field: the id of the feature that gates it. */
    public int tag() {
        return tag;
    }

    /** @return Serialized field value. */
    public byte[] bytes() {
        return bytes;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof RawField))
            return false;

        RawField other = (RawField)o;

        return tag == other.tag && Arrays.equals(bytes, other.bytes);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * tag + Arrays.hashCode(bytes);
    }
}
