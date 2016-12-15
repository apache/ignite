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

package org.apache.ignite.internal.util;

/**
 *
 */
public class PutStatistic extends OperationStatistic {
    /** */
    private static final Ops VALS[] = Ops.values();

    PutStatistic() {
        super(VALS.length);
    }

    /** {@inheritDoc} */
    @Override public String opName(int op) {
        return VALS[op].name();
    }

    /**
     *
     */
    public enum Ops {
        /** */
        UPDATE_INTERNAL,

        /** */
        FIND_ONE,

        /** */
        LOCK,

        /** */
        UNLOCK,

        /** */
        STORE_ADD,

        /** */
        STORE_RMV,

        /** */
        TREE_PUT,

        /** */
        FREE_LIST_FIND,

        /** */
        FREE_LIST_PUT,

        /** */
        DATA_ADD,

        /** */
        DATA_ADD1,

        /** */
        DATA_ADD2,

        /** */
        DATA_ADD3
        ;
    }
}
