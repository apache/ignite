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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.TreeSet;
import org.apache.ignite.springdata.misc.ApplicationConfiguration;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonRepository;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteSpringDataCrudSelfTest extends GridCommonAbstractTest {
    /** Repository. */
    private static PersonRepository repo;

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

        repo = ctx.getBean(PersonRepository.class);
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

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        ctx.destroy();
    }

    /**
     *
     */
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

    /**
     *
     */
    @Test
    public void testPutAllGetAll() {
        LinkedHashMap<Integer, Person> map = new LinkedHashMap<>();

        for (int i = CACHE_SIZE; i < CACHE_SIZE + 50; i++)
            map.put(i, new Person("some_name" + i, "some_surname" + i));

        Iterator<Person> persons = repo.save(map).iterator();

        assertEquals(CACHE_SIZE + 50, repo.count());

        Iterator<Person> origPersons = map.values().iterator();

        while (persons.hasNext())
            assertEquals(origPersons.next(), persons.next());

        try {
            repo.saveAll(map.values());

            fail("Managed to save a list of Persons with ids");
        }
        catch (UnsupportedOperationException e) {
            //expected
        }

        persons = repo.findAllById(map.keySet()).iterator();

        int counter = 0;

        while (persons.hasNext()) {
            persons.next();
            counter++;
        }

        assertEquals(map.size(), counter);
    }

    /**
     *
     */
    @Test
    public void testGetAll() {
        assertEquals(CACHE_SIZE, repo.count());

        Iterator<Person> persons = repo.findAll().iterator();

        int counter = 0;

        while (persons.hasNext()) {
            persons.next();
            counter++;
        }

        assertEquals(repo.count(), counter);
    }

    /**
     *
     */
    @Test
    public void testDelete() {
        assertEquals(CACHE_SIZE, repo.count());

        repo.deleteById(0);

        assertEquals(CACHE_SIZE - 1, repo.count());
        assertEquals(Optional.empty(),repo.findById(0));

        try {
            repo.delete(new Person("", ""));

            fail("Managed to delete a Person without id");
        }
        catch (UnsupportedOperationException e) {
            //expected
        }
    }

    /**
     *
     */
    @Test
    public void testDeleteSet() {
        assertEquals(CACHE_SIZE, repo.count());

        TreeSet<Integer> ids = new TreeSet<>();

        for (int i = 0; i < CACHE_SIZE / 2; i++)
            ids.add(i);

        repo.deleteAllById(ids);

        assertEquals(CACHE_SIZE / 2, repo.count());

        try {
            ArrayList<Person> persons = new ArrayList<>();

            for (int i = 0; i < 3; i++)
                persons.add(new Person(String.valueOf(i), String.valueOf(i)));

            repo.deleteAll(persons);

            fail("Managed to delete Persons without ids");
        }
        catch (UnsupportedOperationException e) {
            //expected
        }
    }

    /**
     *
     */
    @Test
    public void testDeleteAll() {
        assertEquals(CACHE_SIZE, repo.count());

        repo.deleteAll();

        assertEquals(0, repo.count());
    }

    /**
     *
     */
    private void fillInRepository() {
        for (int i = 0; i < CACHE_SIZE; i++) {
            repo.save(i, new Person("person" + Integer.toHexString(i),
                    "lastName" + Integer.toHexString((i + 16) % 256)));
        }
    }
}
