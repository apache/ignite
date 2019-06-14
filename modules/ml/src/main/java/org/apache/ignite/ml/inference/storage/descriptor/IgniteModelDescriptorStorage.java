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

package org.apache.ignite.ml.inference.storage.descriptor;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.inference.ModelDescriptor;

/**
 * Model descriptor storage based on Apache Ignite cache.
 */
public class IgniteModelDescriptorStorage implements ModelDescriptorStorage {
    /** Apache Ignite cache to keep model descriptors. */
    private final IgniteCache<String, ModelDescriptor> models;

    /**
     * Constructs a new instance of Ignite model descriptor storage.
     *
     * @param models Ignite cache with model descriptors.
     */
    public IgniteModelDescriptorStorage(IgniteCache<String, ModelDescriptor> models) {
        this.models = models;
    }

    /** {@inheritDoc} */
    @Override public void put(String mdlId, ModelDescriptor mdl) {
        models.put(mdlId, mdl);
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(String mdlId, ModelDescriptor mdl) {
        return models.putIfAbsent(mdlId, mdl);
    }

    /** {@inheritDoc} */
    @Override public ModelDescriptor get(String mdlId) {
        return models.get(mdlId);
    }

    /** {@inheritDoc} */
    @Override public void remove(String mdlId) {
        models.remove(mdlId);
    }

    /** {@inheritDoc} */
    @Override public Iterator<IgniteBiTuple<String, ModelDescriptor>> iterator() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(models.iterator(), Spliterator.ORDERED), false)
            .map(e -> new IgniteBiTuple<>(e.getKey(), e.getValue()))
            .iterator();

    }
}
