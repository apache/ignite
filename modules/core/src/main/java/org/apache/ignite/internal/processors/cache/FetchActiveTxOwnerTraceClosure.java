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
package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;

/**
 * Closure that is computed on near node to get the stack trace of active transaction owner thread.
 */
public class FetchActiveTxOwnerTraceClosure implements IgniteCallable<String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final long txOwnerThreadId;

    /** */
    public FetchActiveTxOwnerTraceClosure(long txOwnerThreadId) {
        this.txOwnerThreadId = txOwnerThreadId;
    }

    /**
     * Builds the stack trace dump of the transaction owner thread
     *
     * @return stack trace dump string
     * @throws Exception If failed
     */
    @Override public String call() throws Exception {
        GridStringBuilder traceDump = new GridStringBuilder("Stack trace of the transaction owner thread:\n");

        try {
            U.printStackTrace(txOwnerThreadId, traceDump);
        }
        catch (SecurityException | IllegalArgumentException e) {
            traceDump = new GridStringBuilder("Could not get stack trace of the transaction owner thread: " + e.getMessage());
        }

        return traceDump.toString();
    }
}
