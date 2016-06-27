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

package org.apache.ignite.yardstick.cache.load.model.value;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Cache value class
 */
public class Truck extends Car implements Serializable {
    /**
     * Truck capacity
     */
    public double cap;

    /**
     * Empty constructor
     */
    public Truck() {
        // No-op.
    }

    /**
     * @param id identifier
     * @param make mark of truck
     * @param weight weight
     * @param color Color.
     * @param cap capacity
     */
    public Truck(int id, String make, double weight, Color color, double cap) {
        super(id, make, weight, color);
        this.cap = cap;
    }

    /**
     * @return truck capacity
     */
    public double getCap() {
        return cap;
    }

    /**
     * @param cap truck capacity
     */
    public void setCap(double cap) {
        this.cap = cap;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Truck.class, this);
    }
}
