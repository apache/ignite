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
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.inference.ModelDescriptor;

/**
 * Storage that allows to load, keep and get access to model descriptors (see {@link ModelDescriptor}).
 */
public interface ModelDescriptorStorage extends Iterable<IgniteBiTuple<String, ModelDescriptor>> {
    /**
     * Saves the specified model descriptor with the specified model identifier.
     *
     * @param mdlId Model identifier.
     * @param mdl Model descriptor.
     */
    public void put(String mdlId, ModelDescriptor mdl);

    /**
     * Returns model descriptor saved for the specified model identifier.
     *
     * @param mdlId Model identifier.
     * @return Model descriptor.
     */
    public ModelDescriptor get(String mdlId);

    /**
     * Removes model descriptor for the specified model descriptor.
     *
     * @param mdlId Model identifier.
     */
    public void remove(String mdlId);

    /** {@inheritDoc} */
    public Iterator<IgniteBiTuple<String, ModelDescriptor>> iterator();
}