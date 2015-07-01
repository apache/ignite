/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.jmh.model;

import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Created by GridAdmin1234 on 6/25/2015.
 */
public class Person implements Serializable {
    @QuerySqlField (index = true)
    private UUID id;

    @QuerySqlField (index = true)
    private UUID depId;

    @QuerySqlField
    private String firstName;

    @QuerySqlField
    private String lastName;

    @QuerySqlField
    private int rank;

    @QuerySqlField
    private String title;

    @QuerySqlField
    private int age;

    @QuerySqlField
    private int salary;

    private transient AffinityKey<UUID> key;

    public Person(Department dep, String firstName, String lastName, int rank, String title, int age, int salary) {
        id = UUID.randomUUID();
        depId = dep.getId();

        this.firstName = firstName;
        this.lastName = lastName;
        this.rank = rank;
        this.title = title;
        this.age = age;
        this.salary = salary;
    }

    public UUID getId() {
        return id;
    }

    public UUID getDepId() {
        return depId;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public int getRank() {
        return rank;
    }

    public String getTitle() {
        return title;
    }

    public int getAge() {
        return age;
    }

    public int getSalary() {
        return salary;
    }

    public AffinityKey<UUID> getKey() {
        if (key == null)
            key = new AffinityKey<>(id, depId);

        return key;
    }
}
