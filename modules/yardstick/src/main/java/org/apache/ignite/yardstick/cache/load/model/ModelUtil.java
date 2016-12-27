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

package org.apache.ignite.yardstick.cache.load.model;

import java.util.UUID;
import org.apache.ignite.yardstick.cache.load.model.key.Identifier;
import org.apache.ignite.yardstick.cache.load.model.key.Mark;
import org.apache.ignite.yardstick.cache.load.model.value.Car;
import org.apache.ignite.yardstick.cache.load.model.value.Color;
import org.apache.ignite.yardstick.cache.load.model.value.Truck;
import org.apache.ignite.yardstick.cache.model.Account;
import org.apache.ignite.yardstick.cache.model.Organization;
import org.apache.ignite.yardstick.cache.model.Person;
import org.apache.ignite.yardstick.cache.model.Person1;
import org.apache.ignite.yardstick.cache.model.Person2;
import org.apache.ignite.yardstick.cache.model.Person8;

/**
 * Util class for model.
 */
public class ModelUtil {
    /**
     * Classes of keys.
     */
    private static Class[] keyClasses = {
        Double.class,
        Identifier.class,
        Mark.class,
        Integer.class,
        UUID.class,
        String.class
    };

    /**
     * Classes of values.
     */
    private static Class[] valClasses = {
//        Car.class,
//        Truck.class,
        Person.class,
        Organization.class,
        Account.class,
        Person1.class,
        Person2.class,
        Person8.class
    };

    /**
     * Simple of classes.
     * Upper approximate size of value 24 bytes.
     */
    private static Class[] simpleClasses = {
        Account.class,
        Person1.class,
        Person2.class,
    };

    /**
     * Fat of classes.
     * Upper approximate size of value 128 bytes.
     */
    private static Class[] fatClasses = {
        Person.class,
        Person8.class,
        Organization.class
    };

    /**
     * @param clazz Checked class.
     * @return Result.
     */
    public static boolean canCreateInstance(Class clazz) {
        for (Class c: keyClasses) {
            if (c.equals(clazz))
                return true;
        }

        for (Class c: valClasses) {
            if (c.equals(clazz))
                return true;
        }

        return false;
    }

    /**
     * @param c model class
     * @param id object id
     * @return object from model
     */
    public static Object create(Class c, int id) {
        Object res = null;

        switch (c.getSimpleName()) {
            case "Double":
                res = id;
                break;
            case "Identifier":
                res = new Identifier(id, "id " + id);
                break;
            case "Mark":
                res = new Mark(id, UUID.nameUUIDFromBytes(Integer.toString(id).getBytes()));
                break;
            case "Integer":
                res = id;
                break;
            case "UUID":
                res = UUID.nameUUIDFromBytes(Integer.toString(id).getBytes());
                break;
            case "Car":
                int colorCnt = Color.values().length;
                res = new Car(id, "Mark " + id, id / 2.123 * 100, Color.values()[id % colorCnt]);
                break;
            case "Truck":
                int colors = Color.values().length;
                res = new Truck(id, "Mark " + id, id / 2.123 * 100, Color.values()[id % colors], id / 4.123 * 100);
                break;
            case "Person":
                res = new Person(id, id+1, "First Name " + id, "Last Name " + id, id / 2.123 * 100);
                break;
            case "Organization":
                res = new Organization(id, "Organization " + id);
                break;
            case "Account":
                res = new Account(id);
                break;
            case "Person1":
                res = new Person1(id);
                break;
            case "Person2":
                res = new Person2(id);
                break;
            case "Person8":
                res = new Person8(id);
                break;
            case "String":
                res = String.valueOf(id);
        }

        return res;
    }

    /**
     * @return array of key cache classes
     */
    public static Class[] keyClasses() {
        return keyClasses;
    }

    /**
     * @return Array of cache value classes
     */
    public static Class[] valueClasses() {
        return valClasses;
    }

    /**
     * @return Array of cache value simple classes.
     */
    public static Class[] simpleValueClasses() {
        return simpleClasses;
    }

    /**
     * @return Array of cache value fat classes.
     */
    public static Class[] fatValueClasses() {
        return fatClasses;
    }
}
