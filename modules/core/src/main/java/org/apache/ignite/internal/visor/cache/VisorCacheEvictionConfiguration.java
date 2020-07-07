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
import javax.cache.configuration.Factory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.evictionPolicyMaxSize;

/**
 * Data transfer object for eviction configuration properties.
 */
public class VisorCacheEvictionConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Eviction policy. */
    private String plc;

    /** Cache eviction policy max size. */
    private Integer plcMaxSize;

    /** Eviction filter to specify which entries should not be evicted. */
    @Deprecated
    private String filter;

    /** Eviction filter factory to specify which entries should not be evicted. */
    private String filterFactory;

    /**
     * Default constructor.
     */
    public VisorCacheEvictionConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for eviction configuration properties.
     * @param ccfg Cache configuration.
     */
    public VisorCacheEvictionConfiguration(CacheConfiguration ccfg) {
        final Factory evictionPlc = ccfg.getEvictionPolicyFactory();

        plc = compactClass(evictionPlc);
        plcMaxSize = evictionPolicyMaxSize(evictionPlc);
        filter = compactClass(ccfg.getEvictionFilter());
        filterFactory = compactClass(ccfg.getEvictionFilterFactory());
    }

    /**
     * @return Eviction policy.
     */
    @Nullable public String getPolicy() {
        return plc;
    }

    /**
     * @return Cache eviction policy max size.
     */
    @Nullable public Integer getPolicyMaxSize() {
        return plcMaxSize;
    }

    /**
     * @return Eviction filter to specify which entries should not be evicted or {@code null}.
     *
     * @deprecated Use {@link #getFilterFactory()} instead.
     */
    @Deprecated
    @Nullable public String getFilter() {
        return filter;
    }

    /**
     * @return Eviction filter factory to specify which entries should not be evicted or {@code null}
     * or if {@link #getFilter()} should be used instead.
     */
    @Nullable public String getFilterFactory() {
        return filterFactory;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, plc);
        out.writeObject(plcMaxSize);
        U.writeString(out, filter);
        U.writeString(out, filterFactory);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        plc = U.readString(in);
        plcMaxSize = (Integer)in.readObject();
        filter = U.readString(in);
        filterFactory = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheEvictionConfiguration.class, this);
    }
}
