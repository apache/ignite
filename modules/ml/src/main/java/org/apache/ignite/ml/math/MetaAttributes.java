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

package org.apache.ignite.ml.math;

import java.util.Map;

/**
 * Interface provides support for meta attributes on vectors and matrices.
 */
public interface MetaAttributes {
    /**
     * Implementation should return an instance of the map to store meta attributes.
     */
    public Map<String, Object> getMetaStorage();

    /**
     * Gets meta attribute with given name.
     *
     * @param name Name of the vector meta attribute to get.
     * @param <T> Attribute's type.
     */
    @SuppressWarnings("unchecked")
    public default <T> T getAttribute(String name) {
        return (T)getMetaStorage().get(name);
    }

    /**
     * Sets meta attribute with given name and value.
     *
     * @param name Name of the meta attribute.
     * @param val Attribute value.
     * @param <T> Attribute's type.
     */
    public default <T> void setAttribute(String name, T val) {
        getMetaStorage().put(name, val);
    }

    /**
     * Removes meta attribute with given name.
     *
     * @param name Name of the meta attribute.
     * @return {@code true} if attribute was present and was removed, {@code false} otherwise.
     */
    public default boolean removeAttribute(String name) {
        boolean is = getMetaStorage().containsKey(name);

        if (is)
            getMetaStorage().remove(name);

        return is;
    }

    /**
     * Checks if given meta attribute is present.
     *
     * @param name Attribute name to check.
     */
    public default boolean hasAttribute(String name) {
        return getMetaStorage().containsKey(name);
    }
}
