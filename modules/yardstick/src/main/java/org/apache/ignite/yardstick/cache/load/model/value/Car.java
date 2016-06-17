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
 * Value cache class
 */
public class Car implements Serializable {
    /**
     * Car identifier
     */
    int id;

    /**
     *  Car make
     */
    String make;

    /**
     * Car weight
     */
    double weight;

    /**
     * Car color
     */
    Color color;

    /**
     * Empty constructor
     */
    public Car() {
    }

    /**
     * @param id identifier
     * @param make mark of car
     * @param weight weight
     * @param color car color
     */
    public Car(int id, String make, double weight, Color color) {
        this.id = id;
        this.make = make;
        this.weight = weight;
        this.color = color;
    }

    /**
     * @return car identifier
     */
    public int getId() {
        return id;
    }

    /**
     * @param id car identifier
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return car make
     */
    public String getMake() {
        return make;
    }

    /**
     * @param make car make
     */
    public void setMake(String make) {
        this.make = make;
    }

    /**
     * @return car weight
     */
    public double getWeight() {
        return weight;
    }

    /**
     * @param weight car weight
     */
    public void setWeight(double weight) {
        this.weight = weight;
    }

    /**
     * @return color
     */
    public Color getColor() {
        return color;
    }

    /**
     * @param color color
     */
    public void setColor(Color color) {
        this.color = color;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Car.class, this);
    }
}
