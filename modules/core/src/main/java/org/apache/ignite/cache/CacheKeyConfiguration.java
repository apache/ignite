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

package org.apache.ignite.cache;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Configuration defining various aspects of cache keys without explicit usage of annotations on user classes.
 */
public class CacheKeyConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Type name. */
    private String typeName;

    /** Affinity key field name. */
    private String affKeyFieldName;

    /**
     * Creates an empty cache key configuration that should be populated via setters.
     */
    public CacheKeyConfiguration() {
        // Convenience no-op constructor.
    }

    /**
     * Creates cache key configuration with given type name and affinity field name.
     *
     * @param typeName Type name.
     * @param affKeyFieldName Affinity field name.
     */
    public CacheKeyConfiguration(String typeName, String affKeyFieldName) {
        this.typeName = typeName;
        this.affKeyFieldName = affKeyFieldName;
    }

    /**
     * Sets type name for which affinity field name is being defined.
     *
     * @return Type name.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * @param typeName Type name for which affinity field name is being defined.
     */
    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    /**
     * Gets affinity key field name.
     *
     * @return Affinity key field name.
     */
    public String getAffinityKeyFieldName() {
        return affKeyFieldName;
    }

    /**
     * Sets affinity key field name.
     *
     * @param affKeyFieldName Affinity key field name.
     */
    public void setAffinityKeyFieldName(String affKeyFieldName) {
        this.affKeyFieldName = affKeyFieldName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheKeyConfiguration.class, this);
    }
}
