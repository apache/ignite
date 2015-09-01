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

package org.apache.ignite.internal.processors.continuous;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Start request data.
 */
class StartRequestData implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Projection predicate. */
    private IgnitePredicate<ClusterNode> prjPred;

    /** Serialized projection predicate. */
    private byte[] prjPredBytes;

    /** Deployment class name. */
    private String clsName;

    /** Deployment info. */
    private GridDeploymentInfo depInfo;

    /** Handler. */
    private GridContinuousHandler hnd;

    /** Buffer size. */
    private int bufSize;

    /** Time interval. */
    private long interval;

    /** Automatic unsubscribe flag. */
    private boolean autoUnsubscribe;

    /**
     * Required by {@link java.io.Externalizable}.
     */
    public StartRequestData() {
        // No-op.
    }

    /**
     * @param prjPred Serialized projection predicate.
     * @param hnd Handler.
     * @param bufSize Buffer size.
     * @param interval Time interval.
     * @param autoUnsubscribe Automatic unsubscribe flag.
     */
    StartRequestData(@Nullable IgnitePredicate<ClusterNode> prjPred, GridContinuousHandler hnd,
        int bufSize, long interval, boolean autoUnsubscribe) {
        assert hnd != null;
        assert bufSize > 0;
        assert interval >= 0;

        this.prjPred = prjPred;
        this.hnd = hnd;
        this.bufSize = bufSize;
        this.interval = interval;
        this.autoUnsubscribe = autoUnsubscribe;
    }

    /**
     * @param marsh Marshaller.
     * @throws org.apache.ignite.IgniteCheckedException In case of error.
     */
    void p2pMarshal(Marshaller marsh) throws IgniteCheckedException {
        assert marsh != null;

        prjPredBytes = marsh.marshal(prjPred);
    }

    /**
     * @param marsh Marshaller.
     * @param ldr Class loader.
     * @throws org.apache.ignite.IgniteCheckedException In case of error.
     */
    void p2pUnmarshal(Marshaller marsh, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        assert marsh != null;

        assert prjPred == null;
        assert prjPredBytes != null;

        prjPred = marsh.unmarshal(prjPredBytes, ldr);
    }

    /**
     * @return Projection predicate.
     */
    public IgnitePredicate<ClusterNode> projectionPredicate() {
        return prjPred;
    }

    /**
     * @param prjPred New projection predicate.
     */
    public void projectionPredicate(IgnitePredicate<ClusterNode> prjPred) {
        this.prjPred = prjPred;
    }

    /**
     * @return Serialized projection predicate.
     */
    public byte[] projectionPredicateBytes() {
        return prjPredBytes;
    }

    /**
     * @param prjPredBytes New serialized projection predicate.
     */
    public void projectionPredicateBytes(byte[] prjPredBytes) {
        this.prjPredBytes = prjPredBytes;
    }

    /**
     * @return Deployment class name.
     */
    public String className() {
        return clsName;
    }

    /**
     * @param clsName New deployment class name.
     */
    public void className(String clsName) {
        this.clsName = clsName;
    }

    /**
     * @return Deployment info.
     */
    public GridDeploymentInfo deploymentInfo() {
        return depInfo;
    }

    /**
     * @param depInfo New deployment info.
     */
    public void deploymentInfo(GridDeploymentInfo depInfo) {
        this.depInfo = depInfo;
    }

    /**
     * @return Handler.
     */
    public GridContinuousHandler handler() {
        return hnd;
    }

    /**
     * @param hnd New handler.
     */
    public void handler(GridContinuousHandler hnd) {
        this.hnd = hnd;
    }

    /**
     * @return Buffer size.
     */
    public int bufferSize() {
        return bufSize;
    }

    /**
     * @param bufSize New buffer size.
     */
    public void bufferSize(int bufSize) {
        this.bufSize = bufSize;
    }

    /**
     * @return Time interval.
     */
    public long interval() {
        return interval;
    }

    /**
     * @param interval New time interval.
     */
    public void interval(long interval) {
        this.interval = interval;
    }

    /**
     * @return Automatic unsubscribe flag.
     */
    public boolean autoUnsubscribe() {
        return autoUnsubscribe;
    }

    /**
     * @param autoUnsubscribe New automatic unsubscribe flag.
     */
    public void autoUnsubscribe(boolean autoUnsubscribe) {
        this.autoUnsubscribe = autoUnsubscribe;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        boolean b = prjPredBytes != null;

        out.writeBoolean(b);

        if (b) {
            U.writeByteArray(out, prjPredBytes);
            U.writeString(out, clsName);
            out.writeObject(depInfo);
        }
        else
            out.writeObject(prjPred);

        out.writeObject(hnd);
        out.writeInt(bufSize);
        out.writeLong(interval);
        out.writeBoolean(autoUnsubscribe);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean b = in.readBoolean();

        if (b) {
            prjPredBytes = U.readByteArray(in);
            clsName = U.readString(in);
            depInfo = (GridDeploymentInfo)in.readObject();
        }
        else
            prjPred = (IgnitePredicate<ClusterNode>)in.readObject();

        hnd = (GridContinuousHandler)in.readObject();
        bufSize = in.readInt();
        interval = in.readLong();
        autoUnsubscribe = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StartRequestData.class, this);
    }
}