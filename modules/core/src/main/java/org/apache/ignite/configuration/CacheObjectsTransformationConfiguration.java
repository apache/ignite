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

package org.apache.ignite.configuration;

import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Configuration object for Ignite Cache Objects transformation.
 */
@IgniteExperimental
public class CacheObjectsTransformationConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Transformers. */
    private Map<Byte, CacheObjectsTransformer> transformers;

    /** Active transformer. */
    private CacheObjectsTransformer activeTransformer;

    /**
     * Gets transformers.
     */
    public Map<Byte, CacheObjectsTransformer> getTransformers() {
        return transformers;
    }

    /**
     * Sets transformers.
     * @param transformers Transformers.
     */
    public CacheObjectsTransformationConfiguration setTransformers(Map<Byte, CacheObjectsTransformer> transformers) {
        this.transformers = transformers;

        return this;
    }

    /**
     * Gets active transformer.
     */
    public CacheObjectsTransformer getActiveTransformer() {
        return activeTransformer;
    }

    /**
     * Sets active transformer.
     * @param activeTransformer Active transformer.
     */
    public CacheObjectsTransformationConfiguration setActiveTransformer(CacheObjectsTransformer activeTransformer) {
        this.activeTransformer = activeTransformer;

        return this;
    }
}
