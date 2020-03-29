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

package org.apache.ignite.cache.store.jdbc.model;

import java.io.Serializable;

/**
 * BinaryTestKey definition.
 */
public class BinaryTestKey implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for id. */
    private Integer id;

    /**
     * Empty constructor.
     */
    public BinaryTestKey() {
        // No-op.
    }

    /**
     * Full constructor.
     */
    public BinaryTestKey(Integer id) {
        this.id = id;
    }

    /**
     * Gets id.
     *
     * @return Value for id.
     */
    public Integer getId() {
        return id;
    }

    /**
     * Sets id.
     *
     * @param id New value for id.
     */
    public void setId(Integer id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof BinaryTestKey))
            return false;

        BinaryTestKey that = (BinaryTestKey)o;

        return id != null ? id.equals(that.id) : that.id == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "PersonKey [id=" + id +
            "]";
    }
}
