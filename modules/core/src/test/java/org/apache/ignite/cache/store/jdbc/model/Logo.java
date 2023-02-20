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
import java.util.Arrays;

/**
 * Logo definition
 */
public class Logo implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for id. */
    private Integer id;

    /** Logo as byte array */
    private byte[] picture;

    /** Description as value*/
    private String description;

    /**
     * Empty constructor.
     */
    public Logo() {
    }

    /**
     * Full constructor.
     */
    public Logo(Integer id, byte[] picture, String description) {
        this.id = id;
        this.picture = picture;
        this.description = description;
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

    /**
     * Gets picture.
     *
     * @return Value for picture.
     */
    public byte[] getPicture() {
        return picture;
    }

    /**
     * Sets new picture.
     *
     * @param picture New value for picture.
     */
    public void setPicture(byte[] picture) {
        this.picture = picture;
    }

    /**
     * Gets description.
     *
     * @return Value for description.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets description.
     *
     * @param description New value for description.
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof Logo))
            return false;

        Logo that = (Logo)o;

        if (id != null ? !id.equals(that.id) : that.id != null)
            return false;

        if (picture != null ? !Arrays.equals(picture, that.picture) : that.picture != null)
            return false;

        return description != null ? description.equals(that.description) : that.description == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id != null ? id.hashCode() : 0;

        res = 31 * res + (picture != null ? Arrays.hashCode(picture) : 0);

        res = 31 * res + (description != null ? description.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Logo [id=" + id +
            ", picture=" + Arrays.toString(picture) +
            ", description=" + description +
            "]";
    }
}
