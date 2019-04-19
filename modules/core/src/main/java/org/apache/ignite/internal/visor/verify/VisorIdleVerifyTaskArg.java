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

package org.apache.ignite.internal.visor.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Arguments for task {@link VisorIdleVerifyTask}.
 * <br/>
 */
public class VisorIdleVerifyTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Caches. */
    private Set<String> caches;

    /** Exclude caches or groups. */
    private Set<String> excludeCaches;

    /** Check CRC */
    private boolean checkCrc;

    /**
     * Default constructor.
     */
    public VisorIdleVerifyTaskArg() {
        // No-op.
    }

    /**
     * @param caches Caches.
     * @param excludeCaches Exclude caches or group.
     * @param checkCrc Check CRC sum on stored pages on disk.
     */
    public VisorIdleVerifyTaskArg(Set<String> caches, Set<String> excludeCaches, boolean checkCrc) {
        this.caches = caches;
        this.excludeCaches = excludeCaches;
        this.checkCrc = checkCrc;
    }

    /**
     * @param caches Caches.
     * @param checkCrc Check CRC sum on stored pages on disk.
     */
    public VisorIdleVerifyTaskArg(Set<String> caches, boolean checkCrc) {
        this.caches = caches;
        this.checkCrc = checkCrc;
    }

    /**
     * @param caches Caches.
     */
    public VisorIdleVerifyTaskArg(Set<String> caches) {
        this.caches = caches;
    }

    /** */
    public boolean isCheckCrc() {
        return checkCrc;
    }

    /**
     * @return Caches.
     */
    public Set<String> getCaches() {
        return caches;
    }

    /**
     * @return Exclude caches or groups.
     */
    public Set<String> getExcludeCaches() {
        return excludeCaches;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V3;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, caches);

        /**
         * Instance fields since protocol version 2 must be serialized if, and only if class instance isn't child of
         * current class. Otherwise, these fields must be serialized in child class.
         *
         * TODO: https://issues.apache.org/jira/browse/IGNITE-10932 Will remove in 3.0
         */
        if (instanceOfCurrentClass()) {
            U.writeCollection(out, excludeCaches);

            out.writeBoolean(checkCrc);
        }
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        caches = U.readSet(in);

        /**
         * Instance fields since protocol version 2 must be deserialized if, and only if class instance isn't child of
         * current class. Otherwise, these fields must be deserialized in child class.
         *
         * TODO: https://issues.apache.org/jira/browse/IGNITE-10932 Will remove in 3.0
         */
        if (instanceOfCurrentClass()) {
            if (protoVer >= V2)
                excludeCaches = U.readSet(in);

            if (protoVer >= V3)
                checkCrc = in.readBoolean();
        }
    }

    /** */
    protected void excludeCaches(Set<String> excludeCaches) {
        this.excludeCaches = excludeCaches;
    }

    /** */
    protected void checkCrc(boolean checkCrc) {
        this.checkCrc = checkCrc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIdleVerifyTaskArg.class, this);
    }

    /**
     * @return {@code True} if current instance is a instance of current class (not a child class) and {@code False} if
     * current instance is a instance of extented class (i.e child class).
     */
    private boolean instanceOfCurrentClass() {
        return VisorIdleVerifyTaskArg.class == getClass();
    }
}
