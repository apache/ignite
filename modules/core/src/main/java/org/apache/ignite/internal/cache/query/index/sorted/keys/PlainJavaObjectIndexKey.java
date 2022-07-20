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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.jetbrains.annotations.Nullable;

/** */
public class PlainJavaObjectIndexKey extends JavaObjectIndexKey {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private transient Object key;

    /** */
    private byte[] serialized;

    /** */
    public PlainJavaObjectIndexKey(@Nullable Object key, @Nullable byte[] serialized) {
        this.key = key;
        this.serialized = serialized;
    }

    /** {@inheritDoc} */
    @Override public byte[] bytesNoCopy() {
        try {
            if (serialized == null)
                serialized = IndexProcessor.serializer.serialize(key);

            return serialized;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        try {
            if (key == null)
                key = IndexProcessor.serializer.deserialize(serialized);

            return key;

        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to deserialize Java Object from byte array", e);
        }
    }
}
