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
