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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collection;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Base for all messages in replicated cache.
 */
public abstract class GridDistributedBaseMessage extends GridCacheIdMessage implements GridCacheDeployable,
    GridCacheVersionable {
    /** Lock or transaction version. */
    @Order(value = 4, method = "version")
    @GridToStringInclude
    protected GridCacheVersion ver;

    /** Committed versions with order higher than one for this message (needed for commit ordering). */
    @Order(value = 5, method = "committedVersions")
    @GridToStringInclude
    private Collection<GridCacheVersion> committedVers;

    /** Rolled back versions with order higher than one for this message (needed for commit ordering). */
    @Order(value = 6, method = "rolledbackVersions")
    @GridToStringInclude
    private Collection<GridCacheVersion> rolledbackVers;

    /** Count of keys referenced in candidates array (needed only locally for optimization). */
    @GridToStringInclude
    private int cnt;

    /**
     * Empty constructor.
     */
    protected GridDistributedBaseMessage() {
        /* No-op. */
    }

    /**
     * @param cnt Count of keys references in list of candidates.
     * @param addDepInfo Deployment info flag.
     */
    protected GridDistributedBaseMessage(int cnt, boolean addDepInfo) {
        assert cnt >= 0;

        this.cnt = cnt;
        this.addDepInfo = addDepInfo;
    }

    /**
     * @param ver Either lock or transaction version.
     * @param cnt Key count.
     * @param addDepInfo Deployment info flag.
     */
    protected GridDistributedBaseMessage(GridCacheVersion ver, int cnt, boolean addDepInfo) {
        this(cnt, addDepInfo);

        assert ver != null;

        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return Version.
     */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @param ver Version.
     */
    public void version(GridCacheVersion ver) {
        this.ver = ver;
    }

    /**
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     */
    public void completedVersions(Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers) {
        this.committedVers = committedVers;
        this.rolledbackVers = rolledbackVers;
    }

    /**
     * @return Committed versions.
     */
    public Collection<GridCacheVersion> committedVersions() {
        return committedVers;
    }

    /**
     * @param committedVers Committed versions.
     */
    public void committedVersions(Collection<GridCacheVersion> committedVers) {
        this.committedVers = committedVers;
    }

    /**
     * @return Rolled back versions.
     */
    public Collection<GridCacheVersion> rolledbackVersions() {
        return rolledbackVers;
    }

    /**
     * @param rolledbackVers Rolled back versions.
     */
    public void rolledbackVersions(Collection<GridCacheVersion> rolledbackVers) {
        this.rolledbackVers = rolledbackVers;
    }

    /**
     * @return Count of keys referenced in candidates array (needed only locally for optimization).
     */
    int keysCount() {
        return cnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedBaseMessage.class, this, "super", super.toString());
    }
}
