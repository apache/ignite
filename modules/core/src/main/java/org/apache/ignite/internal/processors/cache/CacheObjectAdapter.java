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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 *
 */
public abstract class CacheObjectAdapter implements CacheObject, Externalizable {
    /** */
    @GridToStringInclude
    @GridDirectTransient
    protected Object val;

    /** */
    protected byte[] valBytes;

    /** {@inheritDoc} */
    @Nullable @Override public <T> T getField(String name) {
        // TODO IGNITE-51.
        return null;
    }

    /**
     * @return {@code True} need to copy value returned to user.
     */
    protected boolean needCopy(GridCacheContext ctx) {
        return ctx.config().isCopyOnGet() && val != null && !ctx.portable().immutable(val);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        assert valBytes != null;

        U.writeByteArray(out, valBytes);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        valBytes = U.readByteArray(in);
    }

    /** {@inheritDoc} */
    public String toString() {
        if (byteArray())
            return getClass().getSimpleName() + " [val=<byte array>, len=" + ((byte[])val).length + ']';
        else
            return getClass().getSimpleName() + " [val=" + val + ", hasValBytes=" + (valBytes != null) + ']';
    }
}
