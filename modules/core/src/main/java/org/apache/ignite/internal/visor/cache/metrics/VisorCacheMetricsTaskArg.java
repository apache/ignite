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
 * Task argument for {@link VisorCacheMetricsTask}.
 */
public class VisorCacheMetricsTaskArg extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Flag indicates if the operation should be performed on all user caches or on a specified list. */
    private boolean applyToAllCaches;

    /** Names of a caches which will be affected by task when <tt>applyToAllCaches</tt> is <code>false</code>. */
    private Set<String> cacheNames;

    /** Cache metrics sub-command. */
    private CacheMetricsSubCommand subCmd;

    /**
     * Default constructor.
     */
    public VisorCacheMetricsTaskArg() {
        // No-op.
    }

    /**
     * @param cacheNames Affected cache names.
     */
    public VisorCacheMetricsTaskArg(CacheMetricsSubCommand subCmd, Set<String> cacheNames) {
        this.subCmd = subCmd;
        this.cacheNames = Collections.unmodifiableSet(cacheNames);

        applyToAllCaches = false;
    }

    /**
     * Construct task argument for operations which affect all caches.
     *
     * @param subCmd Operation type.
     */
    public VisorCacheMetricsTaskArg(CacheMetricsSubCommand subCmd) {
        this.subCmd = subCmd;

        applyToAllCaches = true;

        cacheNames = Collections.emptySet();
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(applyToAllCaches);
        U.writeCollection(out, cacheNames);
        U.writeEnum(out, subCmd);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException,
        ClassNotFoundException {
        applyToAllCaches = in.readBoolean();
        cacheNames = U.readSet(in);
        subCmd = U.readEnum(in, CacheMetricsSubCommand.class);
    }

    /**
     * @return Names of a caches which will be affected by task when <tt>applyToAllCaches</tt> is <code>false</code>.
     */
    public Set<String> cacheNames() {
        return Collections.unmodifiableSet(cacheNames);
    }

    /**
     * @return Flag indicates if the operation should be performed on all user caches or on a specified list.
     */
    public boolean applyToAllCaches() {
        return applyToAllCaches;
    }

    /**
     * @return Cache metrics sub-command.
     */
    public CacheMetricsSubCommand subCommand() {
        return subCmd;
    }
}

