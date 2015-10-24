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
import java.util.Collection;

/**
 * Contains list of fields to be indexed. It is possible to provide field name
 * suffixed with index specific extension, for example for {@link Type#SORTED sorted} index
 * the list can be provided as following {@code (id, name asc, age desc)}.
 */
public class QueryEntityIndex implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index name. */
    private String name;

    /** */
    private Collection<String> fields;

    /** */
    private Type type;

    /**
     * Index type.
     */
    public enum Type {
        SORTED, FULLTEXT, GEOSPATIAL
    }

    /**
     * Gets index name. Will be automatically set if not provided by a user.
     *
     * @return Index name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets index name.
     *
     * @param name Index name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets fields included in the index.
     *
     * @return Collection of index fields.
     */
    public Collection<String> getFields() {
        return fields;
    }

    /**
     * Sets fields included in the index.
     *
     * @param fields Collection of index fields.
     */
    public void setFields(Collection<String> fields) {
        this.fields = fields;
    }

    /**
     * Gets index type.
     *
     * @return Index type.
     */
    public Type getType() {
        return type;
    }

    /**
     * Sets index type.
     *
     * @param type Index type.
     */
    public void setType(Type type) {
        this.type = type;
    }
}