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
package org.apache.ignite.internal.processors.query.calcite.schema;

import org.apache.calcite.rel.core.TableModify;

/**
 * POJO to store key, value and modify operation.
 */
public class ModifyTuple {
    /** */
    private final Object key;

    /** */
    private final Object val;

    /** */
    private final TableModify.Operation op;

    /** */
    public ModifyTuple(Object key, Object val, TableModify.Operation op) {
        this.key = key;
        this.val = val;
        this.op = op;
    }

    /** */
    public Object getKey() {
        return key;
    }

    /** */
    public Object getValue() {
        return val;
    }

    /** */
    public TableModify.Operation getOp() {
        return op;
    }
}
