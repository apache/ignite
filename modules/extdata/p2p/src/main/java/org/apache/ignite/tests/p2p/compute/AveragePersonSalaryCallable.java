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

package org.apache.ignite.tests.p2p.compute;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.tests.p2p.cache.Person;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * This closure calculates average salary of person in the defined key range.
 */
public class AveragePersonSalaryCallable implements IgniteCallable<Double> {
    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Cache name. */
    private final String cacheName;

    /** Left range border. */
    private final int from;

    /** Right range border. */
    private final int to;

    /**
     * @param cacheName Cache name.
     * @param from First entry key.
     * @param to Up border of keys.
     */
    public AveragePersonSalaryCallable(String cacheName, int from, int to) {
        this.cacheName = cacheName;
        this.from = from;
        this.to = to;
    }

    /** {@inheritDoc} */
    @Override public Double call() {
        log.info("Job was started with parameters: [node=" + ignite.name() +
            ", cache=" + cacheName +
            ", from=" + from +
            ", to=" + to + ']');

        IgniteCache<Integer, Person> cache = ignite.cache(cacheName);

        if (cache == null)
            return 0D;

        double avgSalary = calculateAverageSalary(cache);

        addPersonWithAverageSalary(cache, avgSalary);

        checkAverageSalaryThroughInvoke(cache, avgSalary);

        if (isTxCache(cache)) {
            log.info("Transaction cache checks was triggered here.");

            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                        double txAvgSalary = calculateAverageSalary(cache);

                        assert Double.compare(txAvgSalary, avgSalary) == 0;
                    }
                }
            }

            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                        addPersonWithAverageSalary(cache, avgSalary);

                        checkAverageSalaryThroughInvoke(cache, avgSalary);
                    }
                }
            }
        }

        return avgSalary;
    }

    /**
     * @param cache Ignite cache.
     * @param avgSalary Average salary calculated previously.
     */
    private void checkAverageSalaryThroughInvoke(IgniteCache<Integer, Person> cache, double avgSalary) {
        double amount = 0;

        for (int i = from; i < to; i++) {
            amount += cache.invoke(i, (MutableEntry<Integer, Person> entry, Object... arguments) ->
                entry.getValue().getSalary());
        }

        assert Double.compare(avgSalary, amount / (to - from)) == 0;
    }

    private boolean isTxCache(IgniteCache<Integer, Person> cache) {
        CacheConfiguration<Integer, Person> ccfg = cache.getConfiguration(CacheConfiguration.class);

        return ccfg.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL;
    }

    /**
     * Adds some person with average salary.
     *
     * @param cache Ignite cache.
     * @param avgSalary Average salary.
     */
    private void addPersonWithAverageSalary(IgniteCache<Integer, Person> cache, double avgSalary) {
        Map<Integer, Person> persons = IntStream.range(from, to).boxed().map(id -> createAveragePerson(avgSalary, to + id))
            .collect(Collectors.toMap(Person::getId, Function.identity(), (u, v) -> {
                    throw new IllegalStateException(String.format("Duplicate key %s", u));
                }, TreeMap::new));

        cache.putAll(persons);

        for (Integer key : persons.keySet()) {
            Person p = cache.getAndPut(to + key, createAveragePerson(avgSalary, to + key));

            assert p == null || Double.compare(avgSalary, p.getSalary()) == 0;
        }
    }

    /**
     * Calculates average salary.
     *
     * @param cache Ignite cache.
     * @return Average salary.
     */
    private double calculateAverageSalary(IgniteCache<Integer, Person> cache) {
        double amount = 0;

        Set<Integer> keys = IntStream.range(from, to).boxed().collect(Collectors.toSet());

        Map<Integer, Person> entries = cache.getAll(keys);

        for (Integer key : keys) {
            Person p = cache.get(key);

            Person p1 = entries.get(key);

            assert p.equals(p1);

            amount += p.getSalary();
        }

        return amount / (to - from);
    }

    /**
     * Creates average person.
     *
     * @param avgSalary Average salary.
     * @param id Id.
     */
    private Person createAveragePerson(double avgSalary, Integer id) {
        Person p = new Person("John " + id);

        p.setId(id);
        p.setLastName("Smith");
        p.setSalary(avgSalary);

        return p;
    }
}
