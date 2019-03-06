/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.examples.springdata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import javax.cache.Cache;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.model.Person;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.domain.PageRequest;

/**
 * The example demonstrates how to interact with an Apache Ignite cluster by means of Spring Data API.
 *
 * Additional cluster nodes can be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start an additional node with {@code examples/config/example-ignite.xml} configuration.
 */
public class SpringDataExample {
    /** Spring Application Context. */
    private static AnnotationConfigApplicationContext ctx;

    /** Ignite Spring Data repository. */
    private static PersonRepository repo;

    /**
     * Executes the example.
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        // Initializing Spring Data context and Ignite repository.
        igniteSpringDataInit();

        populateRepository();

        findPersons();

        queryRepository();

        System.out.println("\n>>> Cleaning out the repository...");

        repo.deleteAll();

        System.out.println("\n>>> Repository size: " + repo.count());

        // Destroying the context.
        ctx.destroy();
    }

    /**
     * Initializes Spring Data and Ignite repositories.
     */
    private static void igniteSpringDataInit() {
        ctx = new AnnotationConfigApplicationContext();

        // Explicitly registering Spring configuration.
        ctx.register(SpringAppCfg.class);

        ctx.refresh();

        // Getting a reference to PersonRepository.
        repo = ctx.getBean(PersonRepository.class);
    }

    /**
     * Fills the repository in with sample data.
     */
    private static void populateRepository() {
        TreeMap<Long, Person> persons = new TreeMap<>();

        persons.put(1L, new Person(1L, 2000L, "John", "Smith", 15000, "Worked for Apple"));
        persons.put(2L, new Person(2L, 2000L, "Brad", "Pitt", 16000, "Worked for Oracle"));
        persons.put(3L, new Person(3L, 1000L, "Mark", "Tomson", 10000, "Worked for Sun"));
        persons.put(4L, new Person(4L, 2000L, "Erick", "Smith", 13000, "Worked for Apple"));
        persons.put(5L, new Person(5L, 1000L, "John", "Rozenberg", 25000, "Worked for RedHat"));
        persons.put(6L, new Person(6L, 2000L, "Denis", "Won", 35000, "Worked for CBS"));
        persons.put(7L, new Person(7L, 1000L, "Abdula", "Adis", 45000, "Worked for NBC"));
        persons.put(8L, new Person(8L, 2000L, "Roman", "Ive", 15000, "Worked for Sun"));

        // Adding data into the repository.
        repo.save(persons);

        System.out.println("\n>>> Added " + repo.count() + " Persons into the repository.");
    }

    /**
     * Gets a list of Persons using standard read operations.
     */
    private static void findPersons() {
        // Getting Person with specific ID.
        Person person = repo.findById(2L).orElse(null);

        System.out.println("\n>>> Found Person [id=" + 2L + ", val=" + person + "]");

        // Getting a list of Persons.

        ArrayList<Long> ids = new ArrayList<>();

        for (long i = 0; i < 5; i++)
            ids.add(i);

        Iterator<Person> persons = repo.findAllById(ids).iterator();

        System.out.println("\n>>> Persons list for specific ids: ");

        while (persons.hasNext())
            System.out.println("   >>>   " + persons.next());
    }

    /**
     * Execute advanced queries over the repository.
     */
    private static void queryRepository() {
        System.out.println("\n>>> Persons with name 'John':");

        List<Person> persons = repo.findByFirstName("John");

        for (Person person: persons)
            System.out.println("   >>>   " + person);


        Cache.Entry<Long, Person> topPerson = repo.findTopByLastNameLike("Smith");

        System.out.println("\n>>> Top Person with surname 'Smith': " + topPerson.getValue());


        List<Long> ids = repo.selectId(1000L, new PageRequest(0, 4));

        System.out.println("\n>>> Persons working for organization with ID > 1000: ");

        for (Long id: ids)
            System.out.println("   >>>   [id=" + id + "]");
    }
}
