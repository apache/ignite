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
 * Cache start arguments.
 */
public class VisorCacheMetricsCollectorTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collect metrics for system caches. */
    private boolean showSysCaches;

    /** Cache names to collect metrics. */
    private List<String> cacheNames;

    /**
     * Default constructor.
     */
    public VisorCacheMetricsCollectorTaskArg() {
        // No-op.
    }

    /**
     * @param showSysCaches Collect metrics for system caches.
     * @param cacheNames Cache names to collect metrics.
     */
    public VisorCacheMetricsCollectorTaskArg(boolean showSysCaches, List<String> cacheNames) {
        this.showSysCaches = showSysCaches;
        this.cacheNames = cacheNames;
    }

    /**
     * @return Collect metrics for system caches
     */
    public boolean isShowSystemCaches() {
        return showSysCaches;
    }

    /**
     * @return Cache names to collect metrics
     */
    public List<String> getCacheNames() {
        return cacheNames;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(showSysCaches);
        U.writeCollection(out, cacheNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        showSysCaches = in.readBoolean();
        cacheNames = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheMetricsCollectorTaskArg.class, this);
    }
}
