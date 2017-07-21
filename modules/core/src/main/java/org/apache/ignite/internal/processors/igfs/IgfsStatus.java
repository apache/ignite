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
 * IGFS response for status request.
 */
public class IgfsStatus implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Total space size. */
    private long spaceTotal;

    /** Used space in IGFS. */
    private long spaceUsed;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsStatus() {
        // No-op.
    }

    /**
     * @param spaceUsed Used space in IGFS.
     * @param spaceTotal Total space available in IGFS.
     */
    public IgfsStatus(long spaceUsed, long spaceTotal) {
        this.spaceUsed = spaceUsed;
        this.spaceTotal = spaceTotal;
    }

    /**
     * @return Total space available in IGFS.
     */
    public long spaceTotal() {
        return spaceTotal;
    }

    /**
     * @return Used space in IGFS.
     */
    public long spaceUsed() {
        return spaceUsed;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(spaceUsed);
        out.writeLong(spaceTotal);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        spaceUsed = in.readLong();
        spaceTotal = in.readLong();
    }
}