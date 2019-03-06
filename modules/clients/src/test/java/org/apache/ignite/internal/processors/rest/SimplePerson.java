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

package org.apache.ignite.internal.processors.rest;

import java.sql.Date;

/**
 * Test class with public fields and without getters and setters.
 */
public class SimplePerson {
    /** Person ID. */
    public int id;

    /** Person name. */
    public String name;

    /** Person birthday. */
    public Date birthday;

    /** Person salary. */
    public double salary;

    /** Must be excluded on serialization. */
    public transient int age;

    /** Post. */
    protected String post;

    /** Bonus. */
    private int bonus;

    /**
     * Default constructor.
     */
    public SimplePerson() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param id Person ID.
     * @param name Person name.
     * @param birthday Person birthday.
     * @param salary Person salary.
     * @param age Person age.
     * @param post Person post.
     * @param bonus Person bonus.
     */
    public SimplePerson(int id, String name, Date birthday, double salary, int age, String post, int bonus) {
        this.id = id;
        this.name = name;
        this.birthday = birthday;
        this.salary = salary;
        this.age = age;
        this.post = post;
        this.bonus = bonus;
    }
}
