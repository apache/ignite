/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
 * Arguments for task {@link VisorIdleVerifyTask}
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
    public Set<String> excludeCaches() {
        return excludeCaches;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V3;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, caches);

        if(instanceOfCurrentClass())
            writeExternalDataFromEndOfObjectOutput(out);
    }

    /**
     * Save new object's specific data (since protocol version 2) to end of output object. It's needs for support
     * backward compatibility in extended (child) classes. At first you must save object's specific data for current
     * class and after save object's specific data for parent class.
     *
     * @see VisorIdleVerifyTaskArg#writeExternalData(ObjectOutput)
     */
    protected void writeExternalDataFromEndOfObjectOutput(ObjectOutput out) throws IOException {
        U.writeCollection(out, excludeCaches);

        out.writeBoolean(checkCrc);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        caches = U.readSet(in);

        if(instanceOfCurrentClass())
            readExternalDataFromEndOfObjectOutput(protoVer, in);
    }

    /**
     * Load new object's specific data content (since protocol version 2) from  end of input object. t's needs for
     * support backward compatibility in extended (child) classes. At first you must load object's specific data for
     * current class and after load object's specific data for parent class.
     *
     * @see VisorIdleVerifyTaskArg#readExternalData(byte, ObjectInput)
     */
    protected void readExternalDataFromEndOfObjectOutput(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        if (protoVer >= V2)
            excludeCaches = U.readSet(in);

        if (protoVer >= V3)
            checkCrc = in.readBoolean();
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
