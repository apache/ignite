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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.typedef.F;

/**
 * JDBC query cursors holder. Used to cancel query.
 */
class JdbcQueryHolder {
    /** Mutex. */
    private final Object mux = new Object();

    /** Query canceled flag. */
    private volatile boolean canceled;

    /** JDBC cursor. */
    private List<JdbcQueryCursor> cursors;

    /**
     * Cancel query.
     */
    void cancel() {
        synchronized (mux) {
            canceled = true;

            if (!F.isEmpty(cursors)) {
                for (JdbcQueryCursor c : cursors)
                    c.close();
            }
        }
    }

    /**
     * @return {@code true} if query is canceled. Otherwise returns {@code false}.
     */
    public boolean isCanceled() {
        return canceled;
    }

    /**
     * @return JDBC cursors.
     */
    List<JdbcQueryCursor> cursors() {
        return cursors;
    }

    /**
     * @param cur JDBC cursor.
     */
    void addCursor(JdbcQueryCursor cur) {
        synchronized (mux) {
            if (cursors == null)
                cursors = new ArrayList<>();

            cursors.add(cur);

            if (canceled)
                cur.close();
        }
    }
}
