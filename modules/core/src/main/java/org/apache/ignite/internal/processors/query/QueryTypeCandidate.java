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

package org.apache.ignite.internal.processors.query;

/**
 * Type candidate which possibly will be registered.
 */
public class QueryTypeCandidate {
    /** Type ID. */
    private final QueryTypeIdKey typeId;

    /** Alternative type ID. */
    private final QueryTypeIdKey altTypeId;

    /** Descriptor. */
    private final QueryTypeDescriptorImpl desc;

    /**
     * Constructor.
     *
     * @param typeId Type ID.
     * @param altTypeId Alternative type ID.
     * @param desc Descriptor.
     */
    public QueryTypeCandidate(QueryTypeIdKey typeId, QueryTypeIdKey altTypeId, QueryTypeDescriptorImpl desc) {
        this.typeId = typeId;
        this.altTypeId = altTypeId;
        this.desc = desc;
    }

    /**
     * @return Type ID.
     */
    public QueryTypeIdKey typeId() {
        return typeId;
    }

    /**
     * @return Alternative type ID.
     */
    public QueryTypeIdKey alternativeTypeId() {
        return altTypeId;
    }

    /**
     * @return Descriptor.
     */
    public QueryTypeDescriptorImpl descriptor() {
        return desc;
    }
}
