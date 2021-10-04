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

package org.apache.ignite.cache.validation;

import java.util.Collection;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;

/** */
public class IgniteCacheTopologyValidator implements TopologyValidator, LifecycleAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Ignite instance. */
    @IgniteInstanceResource
    private transient IgniteEx ignite;

    /** Name of the cache current topology validator belongs to. */
    @CacheNameResource
    private transient String cacheName;

    /** */
    private transient PluggableSegmentationResolver segResolver;

    /** {@inheritDoc} */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        if (ignite.cluster().localNode().isClient())
            return true;

        segResolver.onDoneBeforeTopologyUnlock(ignite.context().cache().context().exchange().lastTopologyFuture());

        boolean isValid = segResolver.isValidSegment();

        if (!isValid)
            U.warn(ignite.log(), "Cache validation failed - current node belongs to segmented part of the cluster." +
                " Cache operation are limited to read-only [cacheName=" + cacheName + ", localNodeId=" +
                ignite.localNode().id() + ']');

        return isValid;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        if (ignite.cluster().localNode().isClient())
            return;

        PluggableSegmentationResolver[] segResolvers = ignite.context().plugins().extensions(PluggableSegmentationResolver.class);

        if (F.isEmpty(segResolvers)) {
            throw new IgniteException("No Ignite plugin that provides " + PluggableSegmentationResolver.class.getName() +
                " extension is found. Cache will be stopped [cacheName=" + cacheName + ']');
        }

        if (segResolvers.length > 1) {
            throw new IgniteException("Multiple Ignite plugins that provide " +
                PluggableSegmentationResolver.class.getName() + " extension are defined. Check your plugins" +
                " configuration and make sure that only one of them provides segmentation resolver. Cache will be" +
                " stopped [cacheName=" + cacheName + ']');
        }

        segResolver = segResolvers[0];
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        // No-op.
    }
}
