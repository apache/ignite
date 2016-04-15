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

package org.apache.ignite.examples.model;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QueryTextField;

/**
 * Person class.
 */
public class Person implements Serializable {
    /** */
    private static final AtomicLong ID_GEN = new AtomicLong();

    /** Person ID (indexed). */
    @QuerySqlField(index = true)
    public Long id;

    /** Organization ID (indexed). */
    @QuerySqlField(index = true)
    public Long orgId;

    /** First name (not-indexed). */
    @QuerySqlField
    public String firstName;

    /** Last name (not indexed). */
    @QuerySqlField
    public String lastName;

    /** Resume text (create LUCENE-based TEXT index for this field). */
    @QueryTextField
    public String resume;

    /** Salary (indexed). */
    @QuerySqlField(index = true)
    public double salary;

    /** Custom cache key to guarantee that person is always collocated with its organization. */
    private transient AffinityKey<Long> key;

    /**
     * Default constructor.
     */
    public Person() {
        // No-op.
    }

    /**
     * Constructs person record.
     *
     * @param org       Organization.
     * @param firstName First name.
     * @param lastName  Last name.
     * @param salary    Salary.
     * @param resume    Resume text.
     */
    public Person(Organization org, String firstName, String lastName, double salary, String resume) {
        // Generate unique ID for this person.
        id = ID_GEN.incrementAndGet();

        orgId = org.id();

        this.firstName = firstName;
        this.lastName = lastName;
        this.salary = salary;
        this.resume = resume;
    }

    /**
     * Constructs person record.
     *
     * @param id Person ID.
     * @param orgId Organization ID.
     * @param firstName First name.
     * @param lastName Last name.
     * @param salary    Salary.
     * @param resume    Resume text.
     */
    public Person(Long id, Long orgId, String firstName, String lastName, double salary, String resume) {
        this.id = id;
        this.orgId = orgId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.salary = salary;
        this.resume = resume;
    }

    /**
     * Constructs person record.
     *
     * @param id Person ID.
     * @param firstName First name.
     * @param lastName Last name.
     */
    public Person(Long id, String firstName, String lastName) {
        this.id = id;

        this.firstName = firstName;
        this.lastName = lastName;
    }

    /**
     * Gets cache affinity key. Since in some examples person needs to be collocated with organization, we create
     * custom affinity key to guarantee this collocation.
     *
     * @return Custom affinity key to guarantee that person is always collocated with organization.
     */
    public AffinityKey<Long> key() {
        if (key == null)
            key = new AffinityKey<>(id, orgId);

        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override public String toString() {
        return "Person [id=" + id +
                ", orgId=" + orgId +
                ", lastName=" + lastName +
                ", firstName=" + firstName +
                ", salary=" + salary +
                ", resume=" + resume + ']';
    }
}
