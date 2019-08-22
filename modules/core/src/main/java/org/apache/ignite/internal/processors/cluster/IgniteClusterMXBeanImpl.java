/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cluster;

import java.util.UUID;
import javax.management.JMException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.IgniteClusterMXBean;

/**
 * Implementation of {@link IgniteClusterMXBean} interface.
 */
public class IgniteClusterMXBeanImpl implements IgniteClusterMXBean {
    /** Cluster instance to delegate method calls to. */
    private final IgniteClusterImpl cluster;

    /**
     * @param cluster Cluster.
     */
    public IgniteClusterMXBeanImpl(IgniteClusterImpl cluster) {
        assert cluster != null;

        this.cluster = cluster;
    }

    /** {@inheritDoc} */
    @Override public UUID getId() {
        return cluster.id();
    }

    /** {@inheritDoc} */
    @Override public String getTag() {
        return cluster.tag();
    }

    /** {@inheritDoc} */
    @Override public void tag(String newTag) throws JMException {
        try {
            cluster.tag(newTag);
        }
        catch (IgniteCheckedException e) {
            throw U.jmException(e);
        }
    }
}
