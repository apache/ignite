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

package org.apache.ignite.internal.processors.igfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * IGFS input stream descriptor - includes stream id and length.
 */
public class IgfsInputStreamDescriptor implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Stream id. */
    private long streamId;

    /** Available length. */
    private long len;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsInputStreamDescriptor() {
        // No-op.
    }

    /**
     * Input stream descriptor constructor.
     *
     * @param streamId Stream id.
     * @param len Available length.
     */
    public IgfsInputStreamDescriptor(long streamId, long len) {
        this.streamId = streamId;
        this.len = len;
    }

    /**
     * @return Stream ID.
     */
    public long streamId() {
        return streamId;
    }

    /**
     * @return Available length.
     */
    public long length() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(streamId);
        out.writeLong(len);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        streamId = in.readLong();
        len = in.readLong();
    }
}