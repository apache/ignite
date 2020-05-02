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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Argument for {@link VisorCacheResetLostPartitionsTask}.
 */
public class VisorCacheResetLostPartitionsTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** List of cache names. */
    private List<String> cacheNames;

    /** Created for toString method because Collection can't be printed. */
    private String modifiedCaches;

    /**
     * Default constructor.
     */
    public VisorCacheResetLostPartitionsTaskArg() {
        // No-op.
    }

    /**
     * @param cacheNames List of cache names.
     */
    public VisorCacheResetLostPartitionsTaskArg(List<String> cacheNames) {
        this.cacheNames = cacheNames;

        if (cacheNames != null)
            modifiedCaches = cacheNames.toString();
    }

    /**
     * @return List of cache names.
     */
    public List<String> getCacheNames() {
        return cacheNames;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, cacheNames);
        U.writeString(out, modifiedCaches);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheNames = U.readList(in);
        modifiedCaches = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheResetLostPartitionsTaskArg.class, this);
    }
}
