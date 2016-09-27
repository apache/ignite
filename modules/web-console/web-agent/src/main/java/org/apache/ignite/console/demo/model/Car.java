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

package org.apache.ignite.console.demo.model;

import java.io.Serializable;

/**
 * Car definition.
 */
public class Car implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for id. */
    private int id;

    /** Value for parkingId. */
    private int parkingId;

    /** Value for name. */
    private String name;

    /**
     * Empty constructor.
     */
    public Car() {
        // No-op.
    }

    /**
     * Full constructor.
     */
    public Car(
        int id,
        int parkingId,
        String name
    ) {
        this.id = id;
        this.parkingId = parkingId;
        this.name = name;
    }

    /**
     * Gets id.
     *
     * @return Value for id.
     */
    public int getId() {
        return id;
    }

    /**
     * Sets id.
     *
     * @param id New value for id.
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Gets parkingId.
     *
     * @return Value for parkingId.
     */
    public int getParkingId() {
        return parkingId;
    }

    /**
     * Sets parkingId.
     *
     * @param parkingId New value for parkingId.
     */
    public void setParkingId(int parkingId) {
        this.parkingId = parkingId;
    }

    /**
     * Gets name.
     *
     * @return Value for name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets name.
     *
     * @param name New value for name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        
        if (!(o instanceof Car))
            return false;

        Car that = (Car)o;

        if (id != that.id)
            return false;

        if (parkingId != that.parkingId)
            return false;

        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id;

        res = 31 * res + parkingId;

        res = 31 * res + (name != null ? name.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Car [id=" + id +
            ", parkingId=" + parkingId +
            ", name=" + name +
            ']';
    }
}
