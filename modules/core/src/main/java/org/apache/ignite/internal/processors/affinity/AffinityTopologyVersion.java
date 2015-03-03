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

package org.apache.ignite.internal.processors.affinity;

import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;

/**
 *
 */
public class AffinityTopologyVersion implements Comparable<AffinityTopologyVersion>, Externalizable {
    /** */
    public static final AffinityTopologyVersion NONE = new AffinityTopologyVersion(-1);

    /** */
    public static final AffinityTopologyVersion ZERO = new AffinityTopologyVersion(0);

    /** */
    private long topVer;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public AffinityTopologyVersion() {
        // No-op.
    }

    /**
     * @param ver Version.
     */
    public AffinityTopologyVersion(long ver) {
        topVer = ver;
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer New topology version.
     */
    public void topologyVersion(long topVer) {
        this.topVer = topVer;
    }

    /**
     *
     */
    public AffinityTopologyVersion previous() {
        return new AffinityTopologyVersion(topVer - 1);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(AffinityTopologyVersion o) {
        return Long.compare(topVer, o.topVer);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return o instanceof AffinityTopologyVersion && topVer == ((AffinityTopologyVersion)o).topVer;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)topVer;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(topVer);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        topVer = in.readLong();
    }

    /**
     * @param msgWriter Message writer.
     */
    public boolean writeTo(MessageWriter msgWriter) {
        return msgWriter.writeLong("topVer.idx", topVer);
    }

    /**
     * @param msgReader Message reader.
     */
    public static AffinityTopologyVersion readFrom(MessageReader msgReader) {
        long topVer = msgReader.readLong("topVer.idx");

        return new AffinityTopologyVersion(topVer);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return String.valueOf(topVer);
    }
}
