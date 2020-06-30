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
package org.apache.ignite.springdata;

import java.util.Collection;

import org.apache.ignite.Ignite;
import org.apache.ignite.springdata.misc.ApplicationConfiguration;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonExpressionRepository;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Test with using repository which is configured by Spring EL
 */
public class IgniteSpringDataCrudSelfExpressionTest extends GridCommonAbstractTest {
    /** Number of entries to store */
    private static final int CACHE_SIZE = 1000;

    /** Repository. */
    private static PersonExpressionRepository repo;

    /** Context. */
    private static AnnotationConfigApplicationContext ctx;

    /** */
    @Rule
    public final ExpectedException expected = ExpectedException.none();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ctx = new AnnotationConfigApplicationContext();
        ctx.register(ApplicationConfiguration.class);
        ctx.refresh();

        repo = ctx.getBean(PersonExpressionRepository.class);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        fillInRepository();

        assertEquals(CACHE_SIZE, repo.count());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        repo.deleteAll();

        assertEquals(0, repo.count());

        super.afterTest();
    }

    /** */
    private void fillInRepository() {
        for (int i = 0; i < CACHE_SIZE - 5; i++) {
            repo.save(i, new Person("person" + Integer.toHexString(i),
                "lastName" + Integer.toHexString((i + 16) % 256)));
        }

        repo.save((int) repo.count(), new Person("uniquePerson", "uniqueLastName"));
        repo.save((int) repo.count(), new Person("nonUniquePerson", "nonUniqueLastName"));
        repo.save((int) repo.count(), new Person("nonUniquePerson", "nonUniqueLastName"));
        repo.save((int) repo.count(), new Person("nonUniquePerson", "nonUniqueLastName"));
        repo.save((int) repo.count(), new Person("nonUniquePerson", "nonUniqueLastName"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        ctx.close();
    }

    /**
     * Tests put & get operations.
     */
    @Test
    public void testPutGet() {
        Person person = new Person("some_name", "some_surname");

        int id = CACHE_SIZE + 1;

        assertEquals(person, repo.save(id, person));

        assertTrue(repo.existsById(id));

        assertEquals(person, repo.findById(id).get());

        expected.expect(UnsupportedOperationException.class);
        expected.expectMessage("Use IgniteRepository.save(key,value) method instead.");
        repo.save(person);
    }

    /**
     * Tests SpEL expression.
     */
    @Test
    public void testCacheCount() {
        Ignite ignite = ctx.getBean("igniteInstance", Ignite.class);

        Collection<String> cacheNames = ignite.cacheNames();

        assertFalse("The SpEL \"#{cacheNames.personCacheName}\" isn't processed!",
            cacheNames.contains("#{cacheNames.personCacheName}"));

        assertTrue("Cache \"PersonCache\" isn't found!",
            cacheNames.contains("PersonCache"));
    }

    /** */
    @Test
    public void testCacheCountTWO() {
        Ignite ignite = ctx.getBean("igniteInstanceTWO", Ignite.class);

        Collection<String> cacheNames = ignite.cacheNames();

        assertFalse("The SpEL \"#{cacheNames.personCacheName}\" isn't processed!",
            cacheNames.contains("#{cacheNames.personCacheName}"));

        assertTrue("Cache \"PersonCache\" isn't found!",
            cacheNames.contains("PersonCache"));
    }
}
