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

package org.apache.ignite.internal.visor.cache.metrics;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Task argument for {@link VisorCacheMetricTask}.
 */
public class VisorCacheMetricTaskArg extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Metric command operation. */
    private CacheMetricOperation operation;

    /** Names of a caches which will be processed. If empty, operation will affect all caches. */
    private Set<String> cacheNames;

    /**
     * Default constructor.
     */
    public VisorCacheMetricTaskArg() {
        // No-op.
    }

    /**
     * @param operation Metric command operation.
     * @param cacheNames Names of the caches, which should be processed.
     */
    public VisorCacheMetricTaskArg(CacheMetricOperation operation, Set<String> cacheNames) {
        this.operation = operation;
        this.cacheNames = cacheNames == null ? null : Collections.unmodifiableSet(cacheNames);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, operation);
        U.writeCollection(out, cacheNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException,
        ClassNotFoundException {
        operation = U.readEnum(in, CacheMetricOperation.class);
        cacheNames = U.readSet(in);
    }

    /**
     * @return Metric command operation.
     */
    public CacheMetricOperation operation() {
        return operation;
    }

    /**
     * @return Names of a caches which will be affected by task when <tt>applyToAllCaches</tt> is <code>false</code>.
     */
    public Set<String> cacheNames() {
        return Collections.unmodifiableSet(cacheNames);
    }

    /**
     * @return Flag indicating that command should be applied to the all user caches.
     */
    public boolean applyToAllCaches() {
        return cacheNames == null;
    }
}

