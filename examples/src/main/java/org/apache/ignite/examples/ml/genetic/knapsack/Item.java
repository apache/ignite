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

package org.apache.ignite.examples.ml.genetic.knapsack;

import java.io.Serializable;

/**
 * POJO to model an Item
 */
public class Item implements Serializable {
    /** weight of item in lbs. */
    private double weight;
    /** value of item */
    private double value;
    /** name of item */
    private String name;

    /**
     * Get the weight
     *
     * @return Weight
     */
    public double getWeight() {
        return weight;
    }

    /**
     * Set the weight
     *
     * @param weight Weight
     */
    public void setWeight(double weight) {
        this.weight = weight;
    }

    /**
     * Get the value
     *
     * @return Value
     */
    public double getValue() {
        return value;
    }

    /**
     * @param value Value
     */
    public void setValue(double value) {
        this.value = value;
    }

    /**
     * Get the name
     *
     * @return Name
     */
    public String getName() {
        return name;
    }

    /**
     * Set the name
     *
     * @param name Name
     */
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Item [weight=" + weight + ", value=" + value + ", name=" + name + "]";
    }

}
