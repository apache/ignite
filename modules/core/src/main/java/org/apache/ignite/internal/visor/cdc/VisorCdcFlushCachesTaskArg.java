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

package org.apache.ignite.internal.visor.cdc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class VisorCdcFlushCachesTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache names. */
    private Set<String> caches;

    /** Only primary flag. */
    private boolean onlyPrimary;

    /** */
    public VisorCdcFlushCachesTaskArg() {
        // No-op.
    }

    /** */
    public VisorCdcFlushCachesTaskArg(Set<String> caches, boolean onlyPrimary) {
        this.caches = caches;
        this.onlyPrimary = onlyPrimary;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, caches);
        out.writeBoolean(onlyPrimary);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        caches = U.readSet(in);
        onlyPrimary = in.readBoolean();
    }

    /** @return Cache names. */
    public Set<String> caches() {
        return caches;
    }

    /** @return Only primary flag. */
    public boolean onlyPrimary() {
        return onlyPrimary;
    }
}
