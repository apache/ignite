/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * <h1 class="header">Full Text Queries</h1>
 * Ignite supports full text queries based on Apache Lucene engine.
 * Note that all fields that are expected to show up in text query results must be annotated with {@link QueryTextField}
 *
 * <h2 class="header">Query usage</h2>
 * Ignite TextQuery supports classic Lucene query syntax.
 * See Lucene classic MultiFieldQueryParser and StandardAnalyzer javadoc for details.
 * As an example, suppose we have data model consisting of {@code 'Employee'} class defined as follows:
 * <pre name="code" class="java">
 * public class Person {
 *     private long id;
 *
 *     private String name;
 *
 *     // Index for text search.
 *     &#64;QueryTextField
 *     private String resume;
 *     ...
 * }
 * </pre>
 *
 * Here is a possible query that will use Lucene text search to scan all resumes to
 * check if employees have {@code Master} degree:
 * <pre name="code" class="java">
 * Query&lt;Cache.Entry&lt;Long, Person&gt;&gt; qry =
 *     new TextQuery(Person.class, "Master");
 *
 * // Query all cache nodes.
 * cache.query(qry).getAll();
 * </pre>
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
        return setType(QueryUtils.typeName(type));
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