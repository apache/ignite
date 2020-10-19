/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.managers.deployment;

import java.lang.reflect.Constructor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Using cache API in P2P tasks.
 */
public class P2PCacheOperationIntoComputeTest extends GridCommonAbstractTest {

    /** Person class name. */
    private static final String PERSON_CLASS_NAME = "org.apache.ignite.tests.p2p.cache.Person";

    /** Deployment task name. */
    private static final String AVERAGE_PERSON_SALARY_CLOSURE_NAME = "org.apache.ignite.tests.p2p.compute.AveragePersonSalaryCallable";

    /** Transactional cache name. */
    private static final String DEFAULT_TX_CACHE_NAME = DEFAULT_CACHE_NAME + "_tx";

    /** Deployment mode for node configuration. */
    public DeploymentMode deplymentMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setPeerClassLoadingEnabled(true)
            .setDeploymentMode(deplymentMode)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME),
                new CacheConfiguration(DEFAULT_TX_CACHE_NAME)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Checks cache API in the deployed tasks with SHARED mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShared() throws Exception {
        deplymentMode = DeploymentMode.SHARED;

        Ignite ignite0 = startGrids(2);

        awaitPartitionMapExchange();

        Ignite client = startClientGrid(2);

        calculateAverageSalary(client, DEFAULT_CACHE_NAME);
        calculateAverageSalary(client, DEFAULT_TX_CACHE_NAME);
    }

    /**
     * Checks cache API in the deployed tasks with CONTINUOUS mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContinuous() throws Exception {
        deplymentMode = DeploymentMode.CONTINUOUS;

        Ignite ignite0 = startGrids(2);

        awaitPartitionMapExchange();

        Ignite client = startClientGrid(2);

        calculateAverageSalary(client, DEFAULT_CACHE_NAME);
        calculateAverageSalary(client, DEFAULT_TX_CACHE_NAME);
    }

    /**
     * Launches a closure which is initiated in a client node, but is executed in server. The closure are manipulating
     * with a data through user's classes.
     *
     * @param client Client node.
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void calculateAverageSalary(
        Ignite client,
        String cacheName
    ) throws Exception {
        Constructor personCtor = getExternalClassLoader().loadClass(PERSON_CLASS_NAME).getConstructor(String.class);

        IgniteCallable<Double> avgSalaryClosure = (IgniteCallable<Double>)getExternalClassLoader().loadClass(AVERAGE_PERSON_SALARY_CLOSURE_NAME)
            .getConstructor(String.class, int.class, int.class).newInstance(cacheName, 0, 10);

        IgniteCache cache = client.cache(cacheName);

        for (int i = 0; i < 10; i++)
            cache.put(i, createPerson(personCtor, i));

        Double avg = client.compute().call(avgSalaryClosure);

        info("Average salary is " + avg);
    }

    /**
     * Creates a new person instance.
     *
     * @param personConst Constructor.
     * @param id Person id.
     * @return A person instance.
     * @throws Exception If failed.
     */
    private Object createPerson(Constructor personConst, int id) throws Exception {
        Object person = personConst.newInstance("Person" + id);
        GridTestUtils.setFieldValue(person, "id", id);
        GridTestUtils.setFieldValue(person, "lastName", "Last name " + id);
        GridTestUtils.setFieldValue(person, "salary", id * Math.PI);
        return person;
    }
}
