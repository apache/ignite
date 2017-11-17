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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/** Simple tuple for a result. {@link org.apache.ignite.internal.util.lang.IgnitePair} with {@code Boolean} too heavy. */
public class LockedModified implements Externalizable {
    /** */
    private static final long serialVersionUID = -5203497119206054926L;

    /** Lock was acquired. */
    boolean locked;

    /** State was modified. */
    boolean modified;

    /**
     * Required by {@link Externalizable}.
     */
    public LockedModified() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param locked Lock was acquired.
     * @param modified State was modified.
     */
    LockedModified(boolean locked, boolean modified) {
        this.locked = locked;
        this.modified = modified;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(locked);
        out.writeBoolean(modified);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        locked = in.readBoolean();
        modified = in.readBoolean();
    }
}
