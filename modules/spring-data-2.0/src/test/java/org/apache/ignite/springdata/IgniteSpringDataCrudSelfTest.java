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
import java.util.Optional;
import java.util.TreeSet;
import javax.cache.CacheException;
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
    /** Number of entries to store */
    private static final int CACHE_SIZE = 1000;

    /** Repository. */
    private static PersonRepository repo;

    /** Repository. */
    private static PersonRepositoryWithCompoundKey repoWithCompoundKey;

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

        repo = ctx.getBean(PersonRepository.class);
        repoWithCompoundKey = ctx.getBean(PersonRepositoryWithCompoundKey.class);
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

    /** */
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
        repo.saveAll(map.values());

        persons = repo.findAllById(map.keySet()).iterator();

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

        repo.deleteById(0);

        assertEquals(CACHE_SIZE - 1, repo.count());
        assertEquals(Optional.empty(),repo.findById(0));

        expected.expect(UnsupportedOperationException.class);
        expected.expectMessage("Use IgniteRepository.deleteById(key) method instead.");
        repo.delete(new Person("", ""));
    }

    /** */
    @Test
    public void testDeleteSet() {
        assertEquals(CACHE_SIZE, repo.count());

        TreeSet<Integer> ids = new TreeSet<>();

        for (int i = 0; i < CACHE_SIZE / 2; i++)
            ids.add(i);

        repo.deleteAllById(ids);

        assertEquals(CACHE_SIZE / 2, repo.count());

        ArrayList<Person> persons = new ArrayList<>();

        for (int i = 0; i < 3; i++)
            persons.add(new Person(String.valueOf(i), String.valueOf(i)));

        expected.expect(UnsupportedOperationException.class);
        expected.expectMessage("Use IgniteRepository.deleteAllById(keys) method instead.");
        repo.deleteAll(persons);
    }

    /** */
    @Test
    public void testDeleteAll() {
        assertEquals(CACHE_SIZE, repo.count());

        repo.deleteAll();

        assertEquals(0, repo.count());
    }

    /**
     * Delete existing record.
     */
    @Test
    public void testDeleteByFirstName() {
        assertEquals(repo.countByFirstNameLike("uniquePerson"), 1);

        long cnt = repo.deleteByFirstName("uniquePerson");

        assertEquals(1, cnt);
    }

    /**
     * Delete NON existing record.
     */
    @Test
    public void testDeleteExpression() {
        long cnt = repo.deleteByFirstName("880");

        assertEquals(0, cnt);
    }

    /**
     * Delete Multiple records due to where.
     */
    @Test
    public void testDeleteExpressionMultiple() {
        long cnt = repo.countByFirstName("nonUniquePerson");
        long cntDeleted = repo.deleteByFirstName("nonUniquePerson");

        assertEquals(cntDeleted, cnt);
    }

    /**
     * Remove should do the same than Delete.
     */
    @Test
    public void testRemoveExpression() {
        repo.removeByFirstName("person3f");

        long cnt = repo.count();
        assertEquals(CACHE_SIZE - 1, cnt);
    }

    /**
     * Delete unique record using lower case key word.
     */
    @Test
    public void testDeleteQuery() {
        repo.deleteBySecondNameLowerCase("uniqueLastName");

        long cntAfter = repo.count();
        assertEquals(CACHE_SIZE - 1, cntAfter);
    }

    /**
     * Try to delete with a wrong @Query.
     */
    @Test
    public void testWrongDeleteQuery() {
        long cntBefore = repo.countByFirstNameLike("person3f");

        expected.expect(CacheException.class);
        repo.deleteWrongByFirstNameQuery("person3f");

        long cntAfter = repo.countByFirstNameLike("person3f");
        assertEquals(cntBefore, cntAfter);
    }

    /**
     * Update with a @Query a record.
     */
    @Test
    public void testUpdateQueryMixedCase() {
        final String newSecondName = "updatedUniqueSecondName";
        int cnt = repo.setFixedSecondNameMixedCase(newSecondName, "uniquePerson");

        assertEquals(1, cnt);

        List<Person> person = repo.findByFirstName("uniquePerson");
        assertEquals(person.get(0).getSecondName(), "updatedUniqueSecondName");
    }

    /**
     * Update with a wrong @Query.
     */
    @Test
    public void testWrongUpdateQuery() {
        expected.expect(CacheException.class);
        int rowsUpdated = repo.setWrongFixedSecondName("updatedUniqueSecondName", "uniquePerson");

        assertEquals(0, rowsUpdated);

        List<Person> person = repo.findByFirstName("uniquePerson");
        assertEquals(person.get(0).getSecondName(), "uniqueLastName");
    }

    /** */
    @Test
    public void shouldDeleteAllById() {
        List<PersonKey> ids = prepareDataWithNonComparableKeys();

        repoWithCompoundKey.deleteAllById(ids);

        assertEquals(0, repoWithCompoundKey.count());
    }

    /** */
    @Test
    public void shouldFindAllById() {
        List<PersonKey> ids = prepareDataWithNonComparableKeys();

        Iterable<Person> res = repoWithCompoundKey.findAllById(ids);

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
}
