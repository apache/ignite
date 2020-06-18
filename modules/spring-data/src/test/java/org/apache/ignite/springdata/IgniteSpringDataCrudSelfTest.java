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
import java.util.List;
import java.util.TreeSet;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.springdata.misc.ApplicationConfiguration;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonKey;
import org.apache.ignite.springdata.misc.PersonRepository;
import org.apache.ignite.springdata.misc.PersonRepositoryWithCompoundKey;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * CRUD tests.
 */
public class IgniteSpringDataCrudSelfTest extends GridCommonAbstractTest {
    /** Number of entries to store. */
    private static final int CACHE_SIZE = 1000;

    /** Context. */
    private static AnnotationConfigApplicationContext ctx;

    /** Repository. */
    private static PersonRepository repo;

    /** Repository. */
    private static PersonRepositoryWithCompoundKey repoWithCompoundKey;

    /** */
    @Rule
    public final ExpectedException expected = ExpectedException.none();

    /** */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ctx = new AnnotationConfigApplicationContext();
        ctx.register(ApplicationConfiguration.class);
        ctx.refresh();

        repo = ctx.getBean(PersonRepository.class);
        repoWithCompoundKey = ctx.getBean(PersonRepositoryWithCompoundKey.class);
        ignite = ctx.getBean(IgniteEx.class);
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
    @Override protected void afterTestsStopped() {
        ctx.destroy();
    }

    /** */
    @Test
    public void testPutGet() {
        Person person = new Person("some_name", "some_surname");

        int id = CACHE_SIZE + 1;

        assertEquals(person, repo.save(id, person));

        assertTrue(repo.exists(id));

        assertEquals(person, repo.findOne(id));

        expected.expect(UnsupportedOperationException.class);
        expected.expectMessage("Use IgniteRepository.save(key,value) method instead.");
        repo.save(person);
    }

    /** */
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

        expected.expect(UnsupportedOperationException.class);
        expected.expectMessage("Use IgniteRepository.save(Map<keys,value>) method instead.");
        repo.save(map.values());

        persons = repo.findAll(map.keySet()).iterator();

        int counter = 0;

        while (persons.hasNext()) {
            persons.next();
            counter++;
        }

        assertEquals(map.size(), counter);
    }

    /** */
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

    /** */
    @Test
    public void testDelete() {
        assertEquals(CACHE_SIZE, repo.count());

        repo.delete(0);

        assertEquals(CACHE_SIZE - 1, repo.count());
        assertNull(repo.findOne(0));

        expected.expect(UnsupportedOperationException.class);
        expected.expectMessage("Use IgniteRepository.delete(key) method instead.");
        repo.delete(new Person("", ""));
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

        expected.expect(UnsupportedOperationException.class);
        expected.expectMessage("Use IgniteRepository.deleteAll(keys) method instead.");
        repo.deleteAll(ids);

        assertEquals(CACHE_SIZE / 2, repo.count());

        ArrayList<Person> persons = new ArrayList<>();

        for (int i = 0; i < 3; i++)
            persons.add(new Person(String.valueOf(i), String.valueOf(i)));

        repo.delete(persons);
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

    /** */
    private void fillInRepository() {
        for (int i = 0; i < CACHE_SIZE; i++)
            repo.save(i, new Person("person" + Integer.toHexString(i),
                "lastName" + Integer.toHexString((i + 16) % 256)));
    }

    /** */
    @Test
    public void shouldDeleteAll() {
        List<PersonKey> ids = prepareDataWithNonComparableKeys();

        repoWithCompoundKey.deleteAll(ids);

        assertEquals(0, repoWithCompoundKey.count());
    }

    /** */
    @Test
    public void shouldFindAll() {
        List<PersonKey> ids = prepareDataWithNonComparableKeys();

        Iterable<Person> res = repoWithCompoundKey.findAll(ids);

        assertEquals(2, res.spliterator().estimateSize());
    }

    /** */
    private List<PersonKey> prepareDataWithNonComparableKeys() {
        List<PersonKey> ids = new ArrayList<>();

        PersonKey key = new PersonKey(1, 1);
        ids.add(key);

        repoWithCompoundKey.save(key, new Person("test1", "test1"));

        key = new PersonKey(2, 2);
        ids.add(key);

        repoWithCompoundKey.save(key, new Person("test2", "test2"));

        assertEquals(2, repoWithCompoundKey.count());

        return ids;
    }

    /** */
    @Test
    public void shouldNotLeakCursorsInRunningQueryManager() {
        RunningQueryManager runningQryMgr = ((IgniteH2Indexing)ignite.context().query().getIndexing()).runningQueryManager();

        assertEquals(0, runningQryMgr.longRunningQueries(0).size());

        List<Person> res = repo.simpleQuery("person0");

        assertEquals(1, res.size());

        assertEquals(0, runningQryMgr.longRunningQueries(0).size());

        Person person = repo.findTopBySecondNameStartingWith("lastName");

        assertNotNull(person);

        assertEquals(0, runningQryMgr.longRunningQueries(0).size());

        long cnt = repo.countByFirstName("person0");

        assertEquals(1, cnt);

        assertEquals(0, runningQryMgr.longRunningQueries(0).size());
    }
}
