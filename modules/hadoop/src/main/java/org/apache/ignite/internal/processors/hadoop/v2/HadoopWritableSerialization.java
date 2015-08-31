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

package org.apache.ignite.internal.processors.hadoop.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Optimized serialization for Hadoop {@link Writable} types.
 */
public class HadoopWritableSerialization implements HadoopSerialization {
    /** */
    private final Class<? extends Writable> cls;

    /**
     * @param cls Class.
     */
    public HadoopWritableSerialization(Class<? extends Writable> cls) {
        assert cls != null;

        this.cls = cls;
    }

    /** {@inheritDoc} */
    @Override public void write(DataOutput out, Object obj) throws IgniteCheckedException {
        assert cls.isAssignableFrom(obj.getClass()) : cls + " " + obj.getClass();

        try {
            ((Writable)obj).write(out);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object read(DataInput in, @Nullable Object obj) throws IgniteCheckedException {
        Writable w = obj == null ? U.newInstance(cls) : cls.cast(obj);

        try {
            w.readFields(in);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        return w;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }
}