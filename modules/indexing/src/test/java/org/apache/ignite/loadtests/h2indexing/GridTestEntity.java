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

package org.apache.ignite.loadtests.h2indexing;

import java.util.Date;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Test entity.
 */
public class GridTestEntity {
    /** */
    @QuerySqlField(index = true)
    private final String name;

    /** */
    @QuerySqlField(index = false)
    private final Date date;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param date Date.
     */
    @SuppressWarnings("AssignmentToDateFieldFromParameter")
    public GridTestEntity(String name, Date date) {
        this.name = name;
        this.date = date;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GridTestEntity that = (GridTestEntity) o;

        return !(date != null ? !date.equals(that.date) : that.date != null) &&
            !(name != null ? !name.equals(that.name) : that.name != null);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = name != null ? name.hashCode() : 0;

        res = 31 * res + (date != null ? date.hashCode() : 0);

        return res;
    }
}