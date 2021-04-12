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

package org.apache.ignite.internal.jdbc2;

import java.util.UUID;
import org.apache.ignite.lang.IgniteCallable;

/**
 * Task for close query cursor on remote node.
 */
class JdbcCloseCursorTask implements IgniteCallable<Void> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Cursor ID to close. */
    private final UUID curId;

    /**
     * @param curId Cursor ID.
     */
    public JdbcCloseCursorTask(UUID curId) {
        this.curId = curId;
    }

    /** {@inheritDoc} */
    @Override public Void call() throws Exception {
        JdbcQueryTask.remove(curId);

        return null;
    }
}
