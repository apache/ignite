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

package org.apache.ignite.springdata.compoundkey;

import java.util.Objects;

/**
 * Value-class
 * */
public class City {
    /** City name */
    private String name;

    /** City district */
    private String district;

    /** City population */
    private int population;

    /**
     * @param name city name
     * @param district city district
     * @param population city population
     * */
    public City(String name, String district, int population) {
        this.name = name;
        this.district = district;
        this.population = population;
    }

    /**
     * @return city name
     * */
    public String getName() {
        return name;
    }

    /**
     * @param name city name
     * */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return city district
     * */
    public String getDistrict() {
        return district;
    }

    /**
     * @param district city district
     * */
    public void setDistrict(String district) {
        this.district = district;
    }

    /**
     * @return city population
     * */
    public int getPopulation() {
        return population;
    }

    /**
     * @param population city population
     * */
    public void setPopulation(int population) {
        this.population = population;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name + " | " + district + " | " + population;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        City city = (City)o;

        return
                Objects.equals(this.name, city.name) &&
                        Objects.equals(this.district, city.district) &&
                        this.population == city.population;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, district, population);
    }
}
