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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCheckedException;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Special field set iterator based on database result set.
 */
public class H2FieldsIterator extends H2ResultSetIterator<List<?>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param data Data.
     * @throws IgniteCheckedException If failed.
     */
    public H2FieldsIterator(ResultSet data) throws IgniteCheckedException {
        super(data);
    }

    /** {@inheritDoc} */
    @Override protected List<?> createRow() {
        ArrayList<Object> res = new ArrayList<>(row.length);

        Collections.addAll(res, row);

        return res;
    }
}
