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

package org.apache.ignite.cache.store.cassandra.session;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.cassandra.persistence.PersistenceController;
import org.apache.ignite.lang.IgniteBiInClosure;

/**
 * Worker for load cache using custom user query.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public class LoadCacheCustomQueryWorker<K, V> implements Callable<Void> {
    /** Cassandra session to execute CQL query */
    private final CassandraSession ses;

    /** User query. */
    private final String qry;

    /** Persistence controller */
    private final PersistenceController ctrl;

    /** Logger */
    private final IgniteLogger log;

    /** Closure for loaded values. */
    private final IgniteBiInClosure<K, V> clo;

    /**
     * @param clo Closure for loaded values.
     */
    public LoadCacheCustomQueryWorker(CassandraSession ses, String qry, PersistenceController ctrl,
        IgniteLogger log, IgniteBiInClosure<K, V> clo) {
        this.ses = ses;
        this.qry = qry.trim().endsWith(";") ? qry : qry + ";";
        this.ctrl = ctrl;
        this.log = log;
        this.clo = clo;
    }

    /** {@inheritDoc} */
    @Override public Void call() throws Exception {
        ses.execute(new BatchLoaderAssistant() {
            /** {@inheritDoc} */
            @Override public String operationName() {
                return "loadCache";
            }

            /** {@inheritDoc} */
            @Override public Statement getStatement() {
                return new SimpleStatement(qry);
            }

            /** {@inheritDoc} */
            @Override public void process(Row row) {
                K key;
                V val;

                try {
                    key = (K)ctrl.buildKeyObject(row);
                }
                catch (Throwable e) {
                    log.error("Failed to build Ignite key object from provided Cassandra row", e);

                    throw new IgniteException("Failed to build Ignite key object from provided Cassandra row", e);
                }

                try {
                    val = (V)ctrl.buildValueObject(row);
                }
                catch (Throwable e) {
                    log.error("Failed to build Ignite value object from provided Cassandra row", e);

                    throw new IgniteException("Failed to build Ignite value object from provided Cassandra row", e);
                }

                clo.apply(key, val);
            }
        });

        return null;
    }
}
