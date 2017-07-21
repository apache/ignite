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

package org.apache.ignite.tests.p2p.cache;

import java.io.Serializable;

/**
 * Person key.
 */
public class PersonKey implements Serializable {
    /** */
    private int id;

    /**
     * Empty constructor for tests.
     */
    public PersonKey() {
        // No-op.
    }

    /**
     * @param id ID.
     */
    public PersonKey(int id) {
        this.id = id;
    }

    /**
     * @return ID.
     */
    public int id() {
        return id;
    }

    /**
     * @param id ID.
     */
    public void id(int id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof PersonKey))
            return false;

        PersonKey key = (PersonKey)o;

        return id == key.id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id;
    }
}
