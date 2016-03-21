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

package org.apache.ignite.internal.processors.cache.version;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 */
public class CacheVersionImplEx extends CacheVersionImpl {
    /** */
    private static final long serialVersionUID = 0L;

    /** DR version. */
    private CacheVersionImpl drVer;

    /**
     *
     */
    public CacheVersionImplEx() {
        // No-op.
    }

    /**
     * @param ver Version.
     * @param drVer DR version.
     */
    public CacheVersionImplEx(CacheVersionImpl ver,  CacheVersionImpl drVer) {
        super(ver.topVer, ver.nodeOrderDrId, ver.globalTime, ver.order);

        assert drVer != null && !(drVer instanceof CacheVersionImplEx) : drVer;

        this.drVer = drVer;
    }

    /**
     * @param topVer Raw topology version field.
     * @param nodeOrderDrId Raw nodeOrderDrId field.
     * @param globalTime Raw time field.
     * @param order Raw order field.
     * @param drVer DR version.
     */
    public CacheVersionImplEx(int topVer, int nodeOrderDrId, long globalTime, long order, CacheVersionImpl drVer) {
        super(topVer, nodeOrderDrId, globalTime, order);

        assert drVer != null && !(drVer instanceof CacheVersionImplEx) : drVer;

        this.drVer = drVer;
    }

    /** {@inheritDoc} */
    @Override public boolean hasConflictVersion() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public CacheVersion conflictVersion() {
        return drVer;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 104;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        drVer = new CacheVersionImpl();

        drVer.readExternal(in);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        drVer.writeExternal(out);
    }

    /** {@inheritDoc} */
    public String toString() {
        return "CacheVersionImplEx [topVer=" + topologyVersion() +
            ", minorTopVer=" + minorTopologyVersion() +
            ", time=" + globalTime() +
            ", order=" + order() +
            ", nodeOrder=" + nodeOrder() +
            ", drVer=" + drVer + ']';
    }
}
