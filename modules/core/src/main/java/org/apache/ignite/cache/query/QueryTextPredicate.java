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

package org.apache.ignite.cache.query;

import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * Predicate for Lucene based fulltext search.
 */
public final class QueryTextPredicate extends QueryPredicate {
    /** */
    private String type;

    /** SQL clause. */
    private String txt;

    /**
     * @param type Type to query.
     * @param txt Search string.
     */
    public QueryTextPredicate(Class<?> type, String txt) {
        setType(type);
        setText(txt);
    }

    /**
     * @param type Type to query.
     * @param txt Search string.
     */
    public QueryTextPredicate(String type, String txt) {
        setType(type);
        setText(txt);
    }

    /**
     * Gets type for query.
     *
     * @return Type.
     */
    public String getType() {
        return type;
    }

    /**
     * Sets type for query.
     *
     * @param type Type.
     */
    public void setType(Class<?> type) {
        setType(type == null ? null : type.getName());
    }

    /**
     * Sets type for query.
     *
     * @param type Type.
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Gets text search string.
     *
     * @return Text search string.
     */
    public String getText() {
        return txt;
    }

    /**
     * Sets text search string.
     *
     * @param txt Text search string.
     */
    public void setText(String txt) {
        this.txt = txt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryTextPredicate.class, this);
    }
}
