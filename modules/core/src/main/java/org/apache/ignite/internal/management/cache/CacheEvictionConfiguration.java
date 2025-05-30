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

package org.apache.ignite.internal.management.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import javax.cache.configuration.Factory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.evictionPolicyMaxSize;

/**
 * Data transfer object for eviction configuration properties.
 */
public class CacheEvictionConfiguration extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Eviction policy. */
    private String plc;

    /** Cache eviction policy max size. */
    private Integer plcMaxSize;

    /** Eviction filter to specify which entries should not be evicted. */
    private String filter;

    /**
     * Default constructor.
     */
    public CacheEvictionConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for eviction configuration properties.
     * @param ccfg Cache configuration.
     */
    public CacheEvictionConfiguration(CacheConfiguration ccfg) {
        final Factory evictionPlc = ccfg.getEvictionPolicyFactory();

        plc = compactClass(evictionPlc);
        plcMaxSize = evictionPolicyMaxSize(evictionPlc);
        filter = compactClass(ccfg.getEvictionFilter());
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
     * @return Eviction filter to specify which entries should not be evicted.
     */
    @Nullable public String getFilter() {
        return filter;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, plc);
        out.writeObject(plcMaxSize);
        U.writeString(out, filter);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        plc = U.readString(in);
        plcMaxSize = (Integer)in.readObject();
        filter = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheEvictionConfiguration.class, this);
    }
}
