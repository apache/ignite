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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Task argument for {@link VisorCacheMetricsTask}.
 */
public class VisorCacheMetricsTaskResult extends IgniteDataTransferObject {
    /** Serial version uid.*/
    private static final long serialVersionUID = 0L;

    /** Processed caches. Is empty for status sub-command.*/
    private Collection<String> processedCaches;

    /** Cache metric modes. Is empty for enable and disable sub-commands.*/
    private Map<String, Boolean> cacheMetricsModes;

    /**
     * Default constructor.
     */
    public VisorCacheMetricsTaskResult() {
        // No-op.
    }

    /**
     * @param cacheMetricsModes Cache metrics modes.
     */
    public VisorCacheMetricsTaskResult(Map<String, Boolean> cacheMetricsModes) {
        this.cacheMetricsModes = Collections.unmodifiableMap(cacheMetricsModes);

        processedCaches = Collections.emptySet();
    }

    /**
     * @param processedCaches Processed caches collection.
     */
    public VisorCacheMetricsTaskResult(Collection<String> processedCaches) {
        this.processedCaches = Collections.unmodifiableCollection(processedCaches);

        cacheMetricsModes = Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, processedCaches);
        U.writeMap(out, cacheMetricsModes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        processedCaches = U.readCollection(in);
        cacheMetricsModes = U.readTreeMap(in);
    }

    /**
     * @return Processed caches. Is empty for status sub-command.
     */
    public Collection<String> processedCaches() {
        return Collections.unmodifiableCollection(processedCaches);
    }

    /**
     * @return Cache metric modes. Is empty for enable and disable sub-commands.
     */
    public Map<String, Boolean> cacheMetricsModes() {
        return Collections.unmodifiableMap(cacheMetricsModes);
    }
}
