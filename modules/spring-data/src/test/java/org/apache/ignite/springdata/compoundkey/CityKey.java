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

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/** Compound key for city class  */
public class CityKey implements Serializable {
    /** city identifier */
    private int ID;

    /** affinity key countrycode  */
    @AffinityKeyMapped
    private String COUNTRYCODE;

    /**
     * @param id city identifier
     * @param countryCode city countrycode
     * */
    public CityKey(int id, String countryCode) {
        this.ID = id;
        this.COUNTRYCODE = countryCode;
    }

    /**
     * @return city id
     * */
    public int getId() {
        return ID;
    }

    /**
     * @return countrycode
     * */
    public String getCountryCode() {
        return COUNTRYCODE;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        CityKey key = (CityKey)o;

        return ID == key.ID &&
                COUNTRYCODE.equals(key.COUNTRYCODE);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(ID, COUNTRYCODE);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return ID + " | " + COUNTRYCODE;
    }
}
