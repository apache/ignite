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

import org.apache.ignite.springdata.misc.ApplicationConfiguration;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonExpressionRepository;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Test with using repository which is configured by Spring EL
 */
public class IgniteSpringDataCrudSelfExpressionTest extends GridCommonAbstractTest {
    /** Repository. */
    private static PersonExpressionRepository repo;

    /** Context. */
    private static AnnotationConfigApplicationContext ctx;

    /** Number of entries to store */
    private static int CACHE_SIZE = 1000;

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

    /**
     *
     */
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
    @Override protected void afterTestsStopped() throws Exception {
        ctx.destroy();
    }

    @Test
    public void testPutGet() {
        Person person = new Person("some_name", "some_surname");

        int id = CACHE_SIZE + 1;

        assertEquals(person, repo.save(id, person));

        assertTrue(repo.existsById(id));

        assertEquals(person, repo.findById(id).get());

        try {
            repo.save(person);

            fail("Managed to save a Person without ID");
        }
        catch (UnsupportedOperationException e) {
            //excepted
        }
    }
}
