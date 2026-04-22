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

package org.apache.ignite.internal.cache.query;

import java.io.Serializable;
import java.util.LinkedHashMap;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/** Message for {@link QueryIndex}. */
public class QueryIndexMessage implements Serializable, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index name. */
    @Order(0)
    public String name;

    /** */
    @GridToStringInclude
    @Order(1)
    public LinkedHashMap<String, Boolean> fields;

    /** */
    @Order(2)
    QueryIndexType type;

    /** */
    @Order(3)
    int inlineSize;

    /** Empty constructor for {@link MessageFactory}. */
    public QueryIndexMessage() {
        // No-op.
    }

    /** Copies {@code idx}. */
    public QueryIndexMessage(QueryIndex idx) {
        name = idx.getName();
        fields = idx.getFields();
        type = idx.getIndexType();
        inlineSize = idx.getInlineSize();
    }

    /** @return Copy of {@code msg} as {@link QueryIndex}. */
    public static QueryIndex queryIndex(QueryIndexMessage msg) {
        QueryIndex res = new QueryIndex();

        res.setName(msg.name);
        res.setFields(msg.fields);
        res.setIndexType(msg.type);
        res.setInlineSize(msg.inlineSize);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndexMessage.class, this);
    }
}
