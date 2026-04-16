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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.marshaller.Marshaller;

/** */
public class FragmentDescription implements MarshallableMessage {
    /** */
    @Order(0)
    long fragmentId;

    /** */
    @Order(1)
    FragmentMapping mapping;

    /** */
    @Order(2)
    Map<Long, List<UUID>> remoteSources;

    /** */
    @Order(3)
    ColocationGroup target;

    /** */
    public FragmentDescription() {
        // No-op.
    }

    /** */
    public FragmentDescription(long fragmentId, FragmentMapping mapping, ColocationGroup target,
        Map<Long, List<UUID>> remoteSources) {
        this.fragmentId = fragmentId;
        this.mapping = mapping;
        this.target = target;
        this.remoteSources = remoteSources;
    }

    /** */
    public long fragmentId() {
        return fragmentId;
    }

    /** */
    public void fragmentId(long fragmentId) {
        this.fragmentId = fragmentId;
    }

    /** */
    public List<UUID> nodeIds() {
        return mapping.nodeIds();
    }

    /** */
    public ColocationGroup target() {
        return target;
    }

    /** */
    public void target(ColocationGroup target) {
        this.target = target;
    }

    /** */
    public Map<Long, List<UUID>> remotes() {
        return remoteSources;
    }

    /** */
    public FragmentMapping mapping() {
        return mapping;
    }

    /** */
    public void mapping(FragmentMapping mapping) {
        this.mapping = mapping;
    }

    /** */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (target != null)
            target = target.explicitMapping();
    }

    /** */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        // No-op.
    }
}
