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

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.hadoop.HadoopTestClassLoader;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopAggregateHistogramExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopAggregateWordCountExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopBbpExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopDBCountPageViewExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopDistBbpExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopDistributedPentominoExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopGrepExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopJoinTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopMultiFileWordCountExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopQuasiMonteCarloTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopSecondarySortExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopTeraSortExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopWordCountExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopWordDeviationExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopWordMeanExampleTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopWordMedianExampleTest;

/**
 * Test suite for Hadoop Map Reduce engine.
 */
public class IgniteHadoopMultiJvmTestSuite extends IgniteHadoopTestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        downloadHadoop();

        final ClassLoader ldr = new HadoopTestClassLoader();

        TestSuite suite = new TestSuite("Ignite Hadoop Multi JVM MR Test Suite");

        suite.addTest(new TestSuite(ldr.loadClass(HadoopBbpExampleTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopDistBbpExampleTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopDistributedPentominoExampleTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopQuasiMonteCarloTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopJoinTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopDBCountPageViewExampleTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopSecondarySortExampleTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopWordCountExampleTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopWordMeanExampleTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopWordDeviationExampleTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopWordMedianExampleTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopAggregateHistogramExampleTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopAggregateWordCountExampleTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopMultiFileWordCountExampleTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopGrepExampleTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopTeraSortExampleTest.class.getName())));

        return suite;
    }
}