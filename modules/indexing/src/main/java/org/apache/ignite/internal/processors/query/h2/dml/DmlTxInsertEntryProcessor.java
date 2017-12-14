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

package org.apache.ignite.internal.processors.query.h2.dml;

import org.apache.ignite.internal.processors.query.IgniteSQLException;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.DUPLICATE_KEY;

/**
 * DML transactional INSERT entry processor.
 */
public class DmlTxInsertEntryProcessor implements EntryProcessor<Object, Object, Void> {
    /** */
    private final Object val;

    /**
     * Constructor.
     *
     * @param val Value to insert.
     */
    public DmlTxInsertEntryProcessor(Object val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<Object, Object> entry, Object... args) throws EntryProcessorException {
        if (entry.exists())
            throw new IgniteSQLException("Duplicate key during INSERT [key=" + entry.getKey() + ']', DUPLICATE_KEY);

        entry.setValue(val);

        return null;
    }
}
