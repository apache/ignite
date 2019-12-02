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

    /**
     * Default constructor.
     */
    public VisorIdleVerifyTaskArg() {
        // No-op.
    }

    /**
     * @param caches Caches.
     * @param excludeCaches Exclude caches or group.
     */
    public VisorIdleVerifyTaskArg(Set<String> caches, Set<String> excludeCaches) {
        this.caches = caches;
        this.excludeCaches = excludeCaches;
    }

    /**
     * @param caches Caches.
     */
    public VisorIdleVerifyTaskArg(Set<String> caches) {
        this.caches = caches;
        this.excludeCaches = excludeCaches;
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
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, caches);
        U.writeCollection(out, excludeCaches);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        caches = U.readSet(in);

        if (protoVer >= V2)
            excludeCaches = U.readSet(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIdleVerifyTaskArg.class, this);
    }
}
