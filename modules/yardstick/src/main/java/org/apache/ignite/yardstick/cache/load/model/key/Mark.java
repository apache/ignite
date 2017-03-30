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
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Cache value class
 */
public class Mark implements Comparable<Mark>, Serializable {
    /**
     * Mark identifier
     */
    private int id;

    /**
     * Mark token
     */
    private UUID token;

    /**
     * Empty constructor
     */
    public Mark() {
        // No-op.
    }

    /**
     * @param id identifier
     * @param token token
     */
    public Mark(int id, UUID token) {
        this.id = id;
        this.token = token;
    }

    /**
     * @return mark identifier
     */
    public int getId() {
        return id;
    }

    /**
     * @param id mark identifier
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return mark token
     */
    public UUID getToken() {
        return token;
    }

    /**
     * @param token mark token
     */
    public void setToken(UUID token) {
        this.token = token;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Mark mark = (Mark)o;

        if (id != mark.id)
            return false;

        return token != null ? token.equals(mark.token) : mark.token == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id;

        res = 31 * res + (token != null ? token.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(Mark o) {
        return id - o.id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Mark.class, this);
    }
}
