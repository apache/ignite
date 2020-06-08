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

import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import org.apache.ignite.springdata.misc.ApplicationConfiguration;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonRepository;
import org.apache.ignite.springdata.misc.QPerson;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.Optional;

/**
 * Tests for query predicate executor implementation.
 */
public class IgniteSpringDataQueryPredicateExecutorImplSelfTest extends GridCommonAbstractTest {
    /** Number of entries to store */
    private static int CACHE_SIZE = 1000;

    /** Repository. */
    private static PersonRepository repo;

    /** Context. */
    private static AnnotationConfigApplicationContext ctx;

    /**
     * Performs context initialization before tests.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ctx = new AnnotationConfigApplicationContext();

        ctx.register(ApplicationConfiguration.class);

        ctx.refresh();

        repo = ctx.getBean(PersonRepository.class);

        for (int i = 0; i < CACHE_SIZE - 5; i++) {
            repo.save(i, new Person("person" + i, "lastName" + i, i));
        }

        repo.save((int) repo.count(), new Person("uniquePerson", "uniqueLastName", 996));
        repo.save((int) repo.count(), new Person("nonUniquePerson", "nonUniqueLastName", 997));
        repo.save((int) repo.count(), new Person("nonUniquePerson", "nonUniqueLastName", 998));
        repo.save((int) repo.count(), new Person("nonUniquePerson", "nonUniqueLastName", 999));
        repo.save((int) repo.count(), new Person("nonUniquePerson", "nonUniqueLastName", 1000));
    }

    /** */
    @Test
    public void testShouldFindOnePerson() {
        String firstName = "person0";
        String secondName = "lastName0";

        Person person = repo.findOne(QPerson.person.firstName.eq(firstName))
            .orElseThrow(() -> new IllegalArgumentException("Person not found by firstName: " + firstName));

        assertEquals(firstName, person.getFirstName());
        assertEquals(secondName, person.getSecondName());
    }

    /** */
    @Test
    public void testShouldNotFindOnePerson() {
        Optional<Person> person = repo.findOne(QPerson.person.firstName.eq("person_not_fount"));

        assertFalse(person.isPresent());
    }

    /** */
    @Test
    public void testShouldFindAllByPredicate() {
        String firstName_1 = "person0";
        String secondName_1 = "lastName0";

        String firstName_2 = "person1";
        String secondName_2 = "lastName1";

        List<Person> persons = (List<Person>) repo.findAll(
            QPerson.person.firstName.eq(firstName_1)
                .or(QPerson.person.firstName.eq(firstName_2))
        );

        assertEquals(2, persons.size());
        assertEquals(1, persons.stream().filter(p -> p.getSecondName().equals(secondName_1)).count());
        assertEquals(1, persons.stream().filter(p -> p.getSecondName().equals(secondName_2)).count());
    }

    /** */
    @Test
    public void testShouldFindAllByComplexPredicate() {
        String firstName_1 = "person0";
        String secondName_1 = "lastName0";

        String firstName_2 = "person1";
        String secondName_2 = "lastName1";

        List<Person> persons = (List<Person>) repo.findAll(
            QPerson.person.firstName.eq(firstName_1)
                .or(QPerson.person.firstName.eq(firstName_2))
                .or(QPerson.person.age.between(100, 105))
                .or(QPerson.person.age.gt(999))
        );

        assertEquals(9, persons.size());
        assertEquals(1, persons.stream().filter(p -> p.getSecondName().equals(secondName_1)).count());
        assertEquals(1, persons.stream().filter(p -> p.getSecondName().equals(secondName_2)).count());
    }

    /** */
    @Test
    public void testShouldFindAllByPredicateAndSort() {
        String firstName_1 = "person0";
        String secondName_1 = "lastName0";

        String firstName_2 = "person1";
        String secondName_2 = "lastName1";

        List<Person> persons = (List<Person>) repo.findAll(
            QPerson.person.firstName.eq(firstName_1)
                .or(QPerson.person.firstName.eq(firstName_2)),
            Sort.by(Sort.Direction.DESC, "firstName")
        );

        assertEquals(2, persons.size());
        assertEquals(secondName_2, persons.get(0).getSecondName());
        assertEquals(secondName_1, persons.get(1).getSecondName());
    }

    /** */
    @Test
    public void testShouldFindAllByPredicateAndOrderSpecifier() {
        String firstName_1 = "person0";
        String secondName_1 = "lastName0";

        String firstName_2 = "person1";
        String secondName_2 = "lastName1";

        List<Person> persons = (List<Person>) repo.findAll(
            QPerson.person.firstName.eq(firstName_1)
                .or(QPerson.person.firstName.eq(firstName_2)),
            new OrderSpecifier<>(Order.ASC, QPerson.person.firstName)
        );

        assertEquals(2, persons.size());
        assertEquals(secondName_1, persons.get(0).getSecondName());
        assertEquals(secondName_2, persons.get(1).getSecondName());
    }

    /** */
    @Test
    public void testShouldFindAllAndSortByOrderSpecifier() {
        String firstName_1 = "uniquePerson";
        String secondName_1 = "uniqueLastName";

        String firstName_2 = "person994";
        String secondName_2 = "lastName994";

        List<Person> persons = (List<Person>) repo.findAll(
            new OrderSpecifier<>(Order.DESC, QPerson.person.firstName)
        );

        assertEquals(1000, persons.size());
        assertEquals(firstName_1, persons.get(0).getFirstName());
        assertEquals(secondName_1, persons.get(0).getSecondName());
        assertEquals(firstName_2, persons.get(1).getFirstName());
        assertEquals(secondName_2, persons.get(1).getSecondName());
    }

    /** */
    @Test
    public void testShouldFindAllByPredicateAndPageByPageRequest() {
        String firstName_1 = "person0";

        String firstName_2 = "person1";
        String secondName_2 = "lastName1";

        Page<Person> page = repo.findAll(
            QPerson.person.firstName.eq(firstName_1)
                .or(QPerson.person.firstName.eq(firstName_2)),
            PageRequest.of(0, 1, Sort.by(Sort.Direction.DESC, "firstName"))
        );

        List<Person> list = page.getContent();

        assertEquals(1, list.size());
        assertEquals(secondName_2, list.get(0).getSecondName());
    }

    /** */
    @Test
    public void testShouldReturnCount() {
        String firstName_1 = "person0";

        String firstName_2 = "person1";

        long cnt = repo.count(
            QPerson.person.firstName.eq(firstName_1)
                .or(QPerson.person.firstName.eq(firstName_2))
        );

        assertEquals(2, cnt);
    }

    /** */
    @Test
    public void testShouldReturnTrueIfPersonExists() {
        String firstName = "person0";

        assertTrue(repo.exists(QPerson.person.firstName.eq(firstName)));
    }

    /** */
    @Test
    public void testShouldReturnFalseIfPersonNotExists() {
        String firstName = "person1111";

        assertFalse(repo.exists(QPerson.person.firstName.eq(firstName)));
    }
}
