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

package org.apache.ignite.internal.cache.query.index.sorted.keys;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;

/** */
public class DecimalIndexKey implements IndexKey {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private BigDecimal key;

    /** */
    public DecimalIndexKey(BigDecimal key) {
        this.key = key;
    }

    /** */
    public DecimalIndexKey() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public int type() {
        return IndexKeyTypes.DECIMAL;
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        BigDecimal okey = (BigDecimal) o.key();

        return key.compareTo(okey);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(key.unscaledValue().longValue());
        out.writeInt(key.scale());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        key = BigDecimal.valueOf(in.readLong(), in.readInt());
    }
}
