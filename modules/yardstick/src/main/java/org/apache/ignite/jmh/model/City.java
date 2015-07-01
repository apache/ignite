/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.jmh.model;

import org.apache.ignite.cache.query.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Created by GridAdmin1234 on 6/25/2015.
 */
public class City implements Serializable {
    @QuerySqlField(index = true)
    private UUID id;

    @QuerySqlField
    private String name;

    @QuerySqlField
    private int population;

    @QuerySqlField
    private int age;

    private String country;

    public City(String name, int population, int age, String country) {
        id = UUID.randomUUID();

        this.name = name;
        this.population = population;
        this.age = age;
        this.country = country;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getPopulation() {
        return population;
    }

    public int getAge() {
        return age;
    }

    public String getCountry() {
        return country;
    }

    @Override public String toString() {
        return "City{" +
            "id=" + id +
            ", name='" + name + '\'' +
            ", population=" + population +
            ", age=" + age +
            ", country='" + country + '\'' +
            '}';
    }
}
