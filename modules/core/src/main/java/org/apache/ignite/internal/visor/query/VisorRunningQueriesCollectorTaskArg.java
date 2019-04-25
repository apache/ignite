/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.query;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Arguments for task {@link VisorRunningQueriesCollectorTask}
 */
public class VisorRunningQueriesCollectorTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Duration to check. */
    private long duration;

    /**
     * Default constructor.
     */
    public VisorRunningQueriesCollectorTaskArg() {
        // No-op.
    }

    /**
     * @param duration Duration to check.
     */
    public VisorRunningQueriesCollectorTaskArg(long duration) {
        this.duration = duration;
    }

    /**
     * @return Duration to check.
     */
    public long getDuration() {
        return duration;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(duration);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        duration = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorRunningQueriesCollectorTaskArg.class, this);
    }
}
