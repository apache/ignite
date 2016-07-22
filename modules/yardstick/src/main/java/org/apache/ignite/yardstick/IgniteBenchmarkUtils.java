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

package org.apache.ignite.yardstick;

import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.yardstick.cache.IgniteSqlQueryBenchmark;
import org.apache.ignite.yardstick.cache.IgniteSqlQueryDistributedJoinBenchmark;
import org.yardstickframework.BenchmarkDriverStartUp;
import org.yardstickframework.BenchmarkServerStartUp;

/**
 * Utils.
 */
public class IgniteBenchmarkUtils {
    /**
     * Utility class constructor.
     */
    private IgniteBenchmarkUtils() {
        // No-op.
    }

    /**
     * @param igniteTx Ignite transaction.
     * @param txConcurrency Transaction concurrency.
     * @param clo Closure.
     * @return Result of closure execution.
     * @throws Exception
     */
    public static <T> T doInTransaction(IgniteTransactions igniteTx, TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation,  Callable<T> clo) throws Exception {
        while (true) {
            try (Transaction tx = igniteTx.txStart(txConcurrency, txIsolation)) {
                T res = clo.call();

                tx.commit();

                return res;
            }
            catch (CacheException e) {
                if (e.getCause() instanceof ClusterTopologyException) {
                    ClusterTopologyException topEx = (ClusterTopologyException)e.getCause();

                    topEx.retryReadyFuture().get();
                }
                else
                    throw e;
            }
            catch (ClusterTopologyException e) {
                e.retryReadyFuture().get();
            }
            catch (TransactionRollbackException | TransactionOptimisticException ignore) {
                // Safe to retry right away.
            }
        }
    }

//    /**
//     * Starts driver for quick benchmarks testing.
//     *
//     * @param args Command line arguments.
//     * @throws Exception If failed.
//     */
//    public static void main(String[] args) throws Exception {
//        String[] args0 = {
//            "-t", "1",
//            "-w", "5",
//            "-d", "5",
//            "-dn", IgniteSqlQueryDistributedJoinBenchmark.class.getSimpleName(),
//            "-r", "100",
//            "-sn", "IgniteNode",
//            "-bcj",
//            "-cfg", "modules/yardstick/config/ignite-localhost-config.xml"};
//
//        BenchmarkDriverStartUp.main(args0);
//    }

    /**
     * Starts driver for quick benchmarks testing.
     *
     * @param args Command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        String[] args0 = {
            "-t", "15",
            "-w", "30",
            "-d", "60",
            "-cl",
            "-pr", "ThroughputLatencyProbe",
            "-dn", IgniteSqlQueryBenchmark.class.getSimpleName(),
            "-r", "100000",
            "-sn", "IgniteNode",
            "-cfg", "modules/yardstick/config/ignite-localhost-config.xml"};

        BenchmarkDriverStartUp.main(args0);
    }
}
