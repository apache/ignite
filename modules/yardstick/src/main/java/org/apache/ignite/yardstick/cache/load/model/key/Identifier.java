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

package org.apache.ignite.yardstick.cache.load.model.key;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Key cache class
 */
public class Identifier implements Comparable<Identifier>, Serializable {
    /**
     * Integer identifier
     */
    private int id;

    /**
     * String identifier
     */
    private String code;

    /**
     * Empty constructor
     */
    public Identifier() {
    }

    /**
     * @param id identifier
     * @param code code
     */
    public Identifier(int id, String code) {
        this.id = id;
        this.code = code;
    }

    /**
     * @return integer identifier
     */
    public int getId() {

        return id;
    }

    /**
     * @param id integer identifier
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return string identifier
     */
    public String getCode() {
        return code;
    }

    /**
     * @param code string identifier
     */
    public void setCode(String code) {
        this.code = code;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Identifier that = (Identifier)o;

        if (id != that.id)
            return false;

        return code != null ? code.equals(that.code) : that.code == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id;
        res = 31 * res + (code != null ? code.hashCode() : 0);
        return res;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(Identifier o) {
        return Integer.compare(id, o.id);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Identifier.class, this);
    }
}
