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
