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

import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query for Lucene based fulltext search.
 *
 * @see IgniteCache#query(Query)
 */
public final class TextQuery<K, V> extends Query<Cache.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private String type;

    /** SQL clause. */
    private String txt;

    /**
     * Constructs query for the given search string.
     *
     * @param type Type.
     * @param txt Search string.
     */
    public TextQuery(String type, String txt) {
        setType(type);
        setText(txt);
    }

    /**
     * Constructs query for the given search string.
     *
     * @param type Type.
     * @param txt Search string.
     */
    public TextQuery(Class<?> type, String txt) {
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
     * @return {@code this} For chaining.
     */
    public TextQuery<K, V> setType(Class<?> type) {
        return setType(GridQueryProcessor.typeName(type));
    }

    /**
     * Sets type for query.
     *
     * @param type Type.
     * @return {@code this} For chaining.
     */
    public TextQuery<K, V> setType(String type) {
        this.type = type;

        return this;
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
     * @return {@code this} For chaining.
     */
    public TextQuery<K, V> setText(String txt) {
        A.notNull(txt, "txt");

        this.txt = txt;

        return this;
    }

    /** {@inheritDoc} */
    @Override public TextQuery<K, V> setPageSize(int pageSize) {
        return (TextQuery<K, V>)super.setPageSize(pageSize);
    }

    /** {@inheritDoc} */
    @Override public TextQuery<K, V> setLocal(boolean loc) {
        return (TextQuery<K, V>)super.setLocal(loc);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TextQuery.class, this);
    }
}