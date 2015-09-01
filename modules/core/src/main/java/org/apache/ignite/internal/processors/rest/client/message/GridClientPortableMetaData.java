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

package org.apache.ignite.internal.processors.rest.client.message;

import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Portable meta data sent from client.
 */
public class GridClientPortableMetaData {
    /** */
    private int typeId;

    /** */
    private String typeName;

    /** */
    private Map<String, Integer> fields;

    /** */
    private String affKeyFieldName;

    /**
     * @return Type ID.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * @return Type name.
     */
    public String typeName() {
        return typeName;
    }

    /**
     * @return Fields.
     */
    public Map<String, Integer> fields() {
        return fields;
    }

    /**
     * @return Affinity key field name.
     */
    public String affinityKeyFieldName() {
        return affKeyFieldName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientPortableMetaData.class, this);
    }
}