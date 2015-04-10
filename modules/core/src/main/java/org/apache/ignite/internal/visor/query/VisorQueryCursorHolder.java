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

package org.apache.ignite.internal.visor.query;

import java.io.*;

/**
 * ResultSet future holder.
 */
public class VisorQueryCursorHolder<T> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query cursor. */
    private final VisorQueryCursor<T> cur;

    /** Flag indicating that this future was read from last check. */
    private volatile boolean accessed;

    /**
     * @param cur Future.
     * @param accessed {@code true} if query was accessed before remove timeout expired.
     */
    public VisorQueryCursorHolder(VisorQueryCursor<T> cur, boolean accessed) {
        this.cur = cur;
        this.accessed = accessed;
    }

    /**
     * @return Query cursor.
     */
    public VisorQueryCursor<T> cursor() {
        return cur;
    }

    /**
     * @return Flag indicating that this future was read from last check..
     */
    public boolean accessed() {
        return accessed;
    }

    /**
     * @param accessed New accessed.
     */
    public void accessed(boolean accessed) {
        this.accessed = accessed;
    }
}
