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

import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.springdata.misc.ApplicationConfiguration;
import org.apache.ignite.springdata.misc.FullNameProjection;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonKey;
import org.apache.ignite.springdata.misc.PersonProjection;
import org.apache.ignite.springdata.misc.PersonRepository;
import org.apache.ignite.springdata.misc.PersonRepositoryWithCompoundKey;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 *
 */
public class IgniteSpringDataCrudSelfTest extends GridCommonAbstractTest {
    /** Repository. */
    private static PersonRepository repo;

    /** Repository. */
    private static PersonRepositoryWithCompoundKey repoWithCompoundKey;

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
     * Delete existing record
     */
    @Test
    public void testDeleteByFirstName() {
        assertEquals(repo.countByFirstNameLike("uniquePerson"), 1);

        long cnt = repo.deleteByFirstName("uniquePerson");

        assertEquals(1, cnt);
    }

    /**
     * Delete NON existing record
     */
    @Test
    public void testDeleteExpression() {
        long cnt = repo.deleteByFirstName("880");

        assertEquals(0, cnt);
    }

    /**
     * Delete Multiple records due to where
     */
    @Test
    public void testDeleteExpressionMultiple() {
        long count = repo.countByFirstName("nonUniquePerson");
        long cnt = repo.deleteByFirstName("nonUniquePerson");

        assertEquals(cnt, count);
    }

    /**
     * Remove should do the same than Delete
     */
    @Test
    public void testRemoveExpression() {
        repo.removeByFirstName("person3f");

        long count = repo.count();
        assertEquals(CACHE_SIZE - 1, count);
    }

    /**
     * Delete unique record using lower case key word
     */
    @Test
    public void testDeleteQuery() {
        repo.deleteBySecondNameLowerCase("uniqueLastName");

        long countAfter = repo.count();
        assertEquals(CACHE_SIZE - 1, countAfter);
    }

    /**
     * Try to delete with a wrong @Query
     */
    @Test
    public void testWrongDeleteQuery() {
        long countBefore = repo.countByFirstNameLike("person3f");

        try {
            repo.deleteWrongByFirstNameQuery("person3f");
        }
        catch (Exception e) {
            //expected
        }

        long countAfter = repo.countByFirstNameLike("person3f");
        assertEquals(countBefore, countAfter);
    }

    /**
     * Update with a @Query a record
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
     * Update with a @Query a record
     */
    @Test
    public void testUpdateQueryMixedCaseProjection() {
        final String newSecondName = "updatedUniqueSecondName1";
        int cnt = repo.setFixedSecondNameMixedCase(newSecondName, "uniquePerson");

        assertEquals(1, cnt);

        List<PersonProjection> person = repo.queryByFirstNameWithProjection("uniquePerson");
        assertEquals(person.get(0).getFullName(), "uniquePerson updatedUniqueSecondName1");
    }
    @Test
    public void testUpdateQueryMixedCaseProjectionNamedParameter() {
        final String newSecondName = "updatedUniqueSecondName2";
        int cnt = repo.setFixedSecondNameMixedCase(newSecondName, "uniquePerson");

        assertEquals(1, cnt);

        List<PersonProjection> person = repo.queryByFirstNameWithProjectionNamedParameter("uniquePerson");
        assertEquals(person.get(0).getFullName(), "uniquePerson updatedUniqueSecondName2");
    }
    @Test
    public void testUpdateQueryMixedCaseDynamicProjectionNamedParameter() {
        final String newSecondName = "updatedUniqueSecondName2";
        int cnt = repo.setFixedSecondNameMixedCase(newSecondName, "uniquePerson");

        assertEquals(1, cnt);

        List<PersonProjection> person = repo.queryByFirstNameWithProjectionNamedParameter(PersonProjection.class, "uniquePerson");
        assertEquals(person.get(0).getFullName(), "uniquePerson updatedUniqueSecondName2");

        List<FullNameProjection> personFullName = repo.queryByFirstNameWithProjectionNamedParameter(FullNameProjection.class, "uniquePerson");
        assertEquals(personFullName.get(0).getFullName(), "uniquePerson updatedUniqueSecondName2");
    }
    @Test
    public void testUpdateQueryOneMixedCaseDynamicProjectionNamedParameter() {
        final String newSecondName = "updatedUniqueSecondName2";
        int cnt = repo.setFixedSecondNameMixedCase(newSecondName, "uniquePerson");

        assertEquals(1, cnt);

        PersonProjection person = repo.queryOneByFirstNameWithProjectionNamedParameter(PersonProjection.class, "uniquePerson");
        assertEquals(person.getFullName(), "uniquePerson updatedUniqueSecondName2");

        FullNameProjection personFullName = repo.queryOneByFirstNameWithProjectionNamedParameter(FullNameProjection.class, "uniquePerson");
        assertEquals(personFullName.getFullName(), "uniquePerson updatedUniqueSecondName2");
    }
    @Test
    public void testUpdateQueryMixedCaseProjectionIndexedParameter() {
        final String newSecondName = "updatedUniqueSecondName3";
        int cnt = repo.setFixedSecondNameMixedCase(newSecondName, "uniquePerson");

        assertEquals(1, cnt);

        List<PersonProjection> person = repo.queryByFirstNameWithProjectionNamedIndexedParameter("notUsed","uniquePerson");
        assertEquals(person.get(0).getFullName(), "uniquePerson updatedUniqueSecondName3");
    }
    @Test
    public void testUpdateQueryMixedCaseProjectionIndexedParameterLuceneTextQuery() {
        final String newSecondName = "updatedUniqueSecondName4";
        int cnt = repo.setFixedSecondNameMixedCase(newSecondName, "uniquePerson");

        assertEquals(1, cnt);

        List<PersonProjection> person = repo.textQueryByFirstNameWithProjectionNamedParameter("uniquePerson");
        assertEquals(person.get(0).getFullName(), "uniquePerson updatedUniqueSecondName4");
    }
    @Test
    public void testUpdateQueryMixedCaseProjectionNamedParameterAndTemplateDomainEntityVariable() {
        final String newSecondName = "updatedUniqueSecondName5";
        int cnt = repo.setFixedSecondNameMixedCase(newSecondName, "uniquePerson");

        assertEquals(1, cnt);

        List<PersonProjection> person = repo.queryByFirstNameWithProjectionNamedParameterAndTemplateDomainEntityVariable("uniquePerson");
        assertEquals(person.get(0).getFullName(), "uniquePerson updatedUniqueSecondName5");
    }
    @Test
    public void testUpdateQueryMixedCaseProjectionNamedParameterWithSpELExtension() {
        final String newSecondName = "updatedUniqueSecondName6";
        int cnt = repo.setFixedSecondNameMixedCase(newSecondName, "uniquePerson");

        assertEquals(1, cnt);

        List<PersonProjection> person = repo.queryByFirstNameWithProjectionNamedParameterWithSpELExtension("uniquePerson");
        assertEquals(person.get(0).getFullName(), "uniquePerson updatedUniqueSecondName6");
        assertEquals(person.get(0).getFirstName(), person.get(0).getFirstNameTransformed());
    }
    /**
     * Update with a wrong @Query
     */
    @Test
    public void testWrongUpdateQuery() {
        final String newSecondName = "updatedUniqueSecondName";
        int rowsUpdated = 0;
        try {
            rowsUpdated = repo.setWrongFixedSecondName(newSecondName, "uniquePerson");
        }
        catch (Exception e) {
            //expected
        }

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

    /** */
    @Test
    public void shouldNotLeakCursorsInRunningQueryManager() {
        RunningQueryManager runningQryMgr = ((IgniteH2Indexing)((IgniteKernal)repo.ignite()).context().query().getIndexing()).runningQueryManager();

        assertEquals(0, runningQryMgr.longRunningQueries(0).size());

        List<Person> res = repo.simpleQuery("person0");

        assertEquals(1, res.size());

        assertEquals(0, runningQryMgr.longRunningQueries(0).size());

        PersonProjection person = repo.findTopBySecondNameStartingWith("lastName");

        assertNotNull(person);

        assertEquals(0, runningQryMgr.longRunningQueries(0).size());

        long cnt = repo.countByFirstName("person0");

        assertEquals(1, cnt);

        assertEquals(0, runningQryMgr.longRunningQueries(0).size());
    }
}
