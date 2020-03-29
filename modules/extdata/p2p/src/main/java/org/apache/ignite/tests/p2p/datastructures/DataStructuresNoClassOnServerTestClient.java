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

package org.apache.ignite.tests.p2p.datastructures;

import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.tests.p2p.NoValueClassOnServerAbstractClient;
import org.apache.ignite.tests.p2p.cache.Person;
import org.apache.ignite.tests.p2p.cache.PersonKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class DataStructuresNoClassOnServerTestClient extends NoValueClassOnServerAbstractClient {
    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    private DataStructuresNoClassOnServerTestClient(String[] args) throws Exception {
        super(args);
    }

    /** {@inheritDoc} */
    @Override protected void runTest() throws Exception {
        testAtomicReference();

        testAtomicStamped();
    }

    /**
     *
     */
    private void testAtomicReference() {
        info("Test atomic reference");

        IgniteAtomicReference<Person> ref = ignite.atomicReference("ref1", null, true);

        assertNull(ref.get());

        ref.set(person("p1"));

        assertEquals(person("p1"), ref.get());

        assertTrue(ref.compareAndSet(person("p1"), person("p2")));

        assertEquals(person("p2"), ref.get());

        assertFalse(ref.compareAndSet(person("p1"), person("p3")));

        assertEquals(person("p2"), ref.get());

        assertTrue(ref.compareAndSet(person("p2"), null));

        assertNull(ref.get());

        assertTrue(ref.compareAndSet(null, person("p2")));

        assertEquals(person("p2"), ref.get());

        ref.close();

        ref = ignite.atomicReference("ref2", person("p1"), true);

        assertEquals(person("p1"), ref.get());
    }

    /**
     *
     */
    private void testAtomicStamped() {
        info("Test atomic stamped");

        IgniteAtomicStamped<Person, PersonKey> stamped = ignite.atomicStamped("s1", null, null, true);

        stamped.set(person("p1"), key(1));

        checkStamped(stamped, "p1", 1);

        assertTrue(stamped.compareAndSet(person("p1"), person("p2"), key(1), key(2)));

        checkStamped(stamped, "p2", 2);

        assertFalse(stamped.compareAndSet(person("p1"), person("p3"), key(1), key(3)));

        checkStamped(stamped, "p2", 2);

        assertFalse(stamped.compareAndSet(person("p2"), person("p3"), key(1), key(3)));

        checkStamped(stamped, "p2", 2);

        assertTrue(stamped.compareAndSet(person("p2"), null, key(2), key(3)));

        checkStamped(stamped, null, 3);

        assertTrue(stamped.compareAndSet(null, person("p2"), key(3), key(4)));

        checkStamped(stamped, "p2", 4);

        stamped.close();

        stamped = ignite.atomicStamped("s2", person("p5"), key(5), true);

        checkStamped(stamped, "p5", 5);
    }

    /**
     * @param stamped Stamped.
     * @param personName Expected person name.
     * @param id Expected stamp.
     */
    private void checkStamped(IgniteAtomicStamped<Person, PersonKey> stamped, String personName, int id) {
        assertEquals(person(personName), stamped.value());
        assertEquals(key(id), stamped.stamp());

        IgniteBiTuple<Person, PersonKey> t = stamped.get();

        assertEquals(person(personName), t.get1());
        assertEquals(key(id), t.get2());
    }

    /**
     * @param name Person name.
     * @return Person instance.
     */
    private Person person(String name) {
        if (name == null)
            return null;

        Person p = new Person();

        p.setName(name);

        return p;
    }

    /**
     * @param id Key ID.
     * @return Key.
     */
    private PersonKey key(int id) {
        return new PersonKey(id);
    }

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (DataStructuresNoClassOnServerTestClient client = new DataStructuresNoClassOnServerTestClient(args)) {
            client.runTest();
        }
        catch (Throwable e) {
            System.out.println("Unexpected error: " + e);

            e.printStackTrace(System.out);

            System.exit(1);
        }
    }
}
