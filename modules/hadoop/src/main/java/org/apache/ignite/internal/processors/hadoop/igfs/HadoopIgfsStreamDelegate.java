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

package org.apache.ignite.internal.processors.hadoop.igfs;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * IGFS Hadoop stream descriptor.
 */
public class HadoopIgfsStreamDelegate {
    /** RPC handler. */
    private final HadoopIgfsEx hadoop;

    /** Target. */
    private final Object target;

    /** Optional stream length. */
    private final long len;

    /**
     * Constructor.
     *
     * @param target Target.
     */
    public HadoopIgfsStreamDelegate(HadoopIgfsEx hadoop, Object target) {
        this(hadoop, target, -1);
    }

    /**
     * Constructor.
     *
     * @param target Target.
     * @param len Optional length.
     */
    public HadoopIgfsStreamDelegate(HadoopIgfsEx hadoop, Object target, long len) {
        assert hadoop != null;
        assert target != null;

        this.hadoop = hadoop;
        this.target = target;
        this.len = len;
    }

    /**
     * @return RPC handler.
     */
    public HadoopIgfsEx hadoop() {
        return hadoop;
    }

    /**
     * @return Stream target.
     */
    @SuppressWarnings("unchecked")
    public <T> T target() {
        return (T) target;
    }

    /**
     * @return Length.
     */
    public long length() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return System.identityHashCode(target);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj != null && obj instanceof HadoopIgfsStreamDelegate &&
            target == ((HadoopIgfsStreamDelegate)obj).target;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopIgfsStreamDelegate.class, this);
    }
}