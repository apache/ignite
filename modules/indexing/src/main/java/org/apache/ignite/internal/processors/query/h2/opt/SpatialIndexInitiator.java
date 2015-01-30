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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.*;
import org.h2.index.*;
import org.h2.table.*;

import java.lang.reflect.*;

/**
 * Util class for SpatialIndex instantiation. Do instantiation via reflection mechanism.
 */
public class SpatialIndexInitiator {
    /** Class for instantiation. */
    private static final String CLASS_NAME = "org.apache.ignite.internal.processors.query.h2.opt.GridH2SpatialIndex_unknown_class";

    /**
     * @param tbl Table.
     * @param idxName Index name.
     * @param cols Columns.
     * @param keyCol Key column.
     * @param valCol Value column.
     */
    public static SpatialIndex createH2SpatialIndex(Table tbl, String idxName, IndexColumn[] cols, int keyCol, int valCol) {
        try {
            Class<?> cls = Class.forName(CLASS_NAME);

            Constructor<?> ctor = cls.getConstructor(Table.class, String.class, IndexColumn[].class, int.class, int.class);

            return (SpatialIndex)ctor.newInstance(tbl, idxName, cols, keyCol, valCol);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate: " + CLASS_NAME, e);
        }
    }
}
