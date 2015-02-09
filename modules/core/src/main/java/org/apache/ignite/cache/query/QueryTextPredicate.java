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

import javax.cache.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public final class QueryTextPredicate<K, V> extends QueryPredicate<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** SQL clause. */
    private String txt;

    /** Arguments. */
    private Object[] args;

    public QueryTextPredicate(String txt, Object... args) {
        this.txt = txt;
        this.args = args;
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

    /**
     * Gets text search arguments.
     *
     * @return Text search arguments.
     */
    public Object[] getArgs() {
        return args;
    }

    /**
     * Sets text search arguments.
     *
     * @param args Text search arguments.
     */
    public void setArgs(Object... args) {
        this.args = args;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(Cache.Entry<K, V> entry) {
        return false; // Not used.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryTextPredicate.class, this);
    }
}
