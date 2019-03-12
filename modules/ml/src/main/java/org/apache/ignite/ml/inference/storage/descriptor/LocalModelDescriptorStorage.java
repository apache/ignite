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

package org.apache.ignite.ml.inference.storage.descriptor;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.inference.ModelDescriptor;

/**
 * Model descriptor storage based on local hash map.
 */
public class LocalModelDescriptorStorage implements ModelDescriptorStorage {
    /** Hash map model storage. */
    private final Map<String, ModelDescriptor> models = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void put(String name, ModelDescriptor mdl) {
        models.put(name, mdl);
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(String mdlId, ModelDescriptor mdl) {
        return models.putIfAbsent(mdlId, mdl) == null;
    }

    /** {@inheritDoc} */
    @Override public ModelDescriptor get(String name) {
        return models.get(name);
    }

    /** {@inheritDoc} */
    @Override public void remove(String name) {
        models.remove(name);
    }

    /** {@inheritDoc} */
    @Override public Iterator<IgniteBiTuple<String, ModelDescriptor>> iterator() {
        return models.entrySet().stream().map(e -> new IgniteBiTuple<>(e.getKey(), e.getValue())).iterator();
    }
}
