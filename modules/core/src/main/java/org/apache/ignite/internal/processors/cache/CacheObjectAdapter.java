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

import java.io.*;

/**
 *
 */
public abstract class CacheObjectAdapter implements CacheObject, Externalizable {
    /** */
    public static final byte TYPE_REGULAR = 1;

    /** */
    public static final byte TYPE_BYTE_ARR = 2;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    protected Object val;

    /** */
    protected byte[] valBytes;

    /**
     * @param ctx Context.
     * @return {@code True} need to copy value returned to user.
     */
    protected boolean needCopy(CacheObjectContext ctx) {
        return ctx.copyOnGet() && val != null && !ctx.processor().immutable(val);
    }

    /**
     * @return {@code True} if value is byte array.
     */
    protected abstract boolean byteArray();

    /** {@inheritDoc} */
    @Override public byte type() {
        return byteArray() ? TYPE_BYTE_ARR : TYPE_REGULAR;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        byte[] valBytes = byteArray() ? (byte[])val : this.valBytes;

        assert valBytes != null;

        out.writeBoolean(byteArray());

        U.writeByteArray(out, valBytes);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean byteArr = in.readBoolean();

        byte[] valBytes = U.readByteArray(in);

        if (byteArr)
            val = valBytes;
        else
            this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    public String toString() {
        if (byteArray())
            return getClass().getSimpleName() + " [val=<byte array>, len=" + ((byte[])val).length + ']';
        else
            return getClass().getSimpleName() + " [val=" + val + ", hasValBytes=" + (valBytes != null) + ']';
    }
}
