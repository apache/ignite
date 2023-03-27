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
package org.apache.ignite.snippets;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class WorkingWithBinaryObjects {

    public static void runAll() {
        binaryObjectsDemo();
        binaryFieldsExample();
        configuringBinaryObjects();
    }

    public static void binaryObjectsDemo() {
        try (Ignite ignite = Ignition.start()) {
            IgniteCache<Integer, Person> cache = ignite.createCache("personCache");

            //tag::enablingBinary[]
            // Create a regular Person object and put it into the cache.
            Person person = new Person(1, "FirstPerson");
            ignite.cache("personCache").put(1, person);

            // Get an instance of binary-enabled cache.
            IgniteCache<Integer, BinaryObject> binaryCache = ignite.cache("personCache").withKeepBinary();
            BinaryObject binaryPerson = binaryCache.get(1);
            //end::enablingBinary[]
            System.out.println("Binary object:" + binaryPerson);

            //tag::binaryBuilder[]
            BinaryObjectBuilder builder = ignite.binary().builder("org.apache.ignite.snippets.Person");

            builder.setField("id", 2L);
            builder.setField("name", "SecondPerson");

            binaryCache.put(2, builder.build());
            //end::binaryBuilder[]
            System.out.println("Value from binary builder:" + cache.get(2).getName());

            //tag::cacheEntryProc[]
            // The EntryProcessor is to be executed for this key.
            int key = 1;
            ignite.cache("personCache").<Integer, BinaryObject>withKeepBinary().invoke(key, (entry, arguments) -> {
                // Create a builder from the old value.
                BinaryObjectBuilder bldr = entry.getValue().toBuilder();

                //Update the field in the builder.
                bldr.setField("name", "Ignite");

                // Set new value to the entry.
                entry.setValue(bldr.build());

                return null;
            });
            //end::cacheEntryProc[]
            System.out.println("EntryProcessor output:" + cache.get(1).getName());
        }
    }

    public static void binaryFieldsExample() {
        try (Ignite ignite = Ignition.start()) {
            //tag::binaryField[]
            Collection<BinaryObject> persons = getPersons();

            BinaryField salary = null;
            double total = 0;
            int count = 0;

            for (BinaryObject person : persons) {
                if (salary == null) {
                    salary = person.type().field("salary");
                }

                total += (float) salary.value(person);
                count++;
            }

            double avg = total / count;
            //end::binaryField[]
            System.out.println("binary fields example:" + avg);
        }
    }

    private static Collection<BinaryObject> getPersons() {
        IgniteBinary binary = Ignition.ignite().binary();
        Person p1 = new Person(1, "name1");
        p1.setSalary(1);
        Person p2 = new Person(2, "name2");
        p2.setSalary(2);
        return Arrays.asList(binary.toBinary(p1), binary.toBinary(p2));
    }

    public static void configuringBinaryObjects() {
        //tag::cfg[]
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        BinaryConfiguration binaryConf = new BinaryConfiguration();
        binaryConf.setNameMapper(new MyBinaryNameMapper());
        binaryConf.setIdMapper(new MyBinaryIdMapper());

        BinaryTypeConfiguration binaryTypeCfg = new BinaryTypeConfiguration();
        binaryTypeCfg.setTypeName("org.apache.ignite.snippets.*");
        binaryTypeCfg.setSerializer(new ExampleSerializer());

        binaryConf.setTypeConfigurations(Collections.singleton(binaryTypeCfg));

        igniteCfg.setBinaryConfiguration(binaryConf);
        //end::cfg[]

    }

    private static class MyBinaryNameMapper implements BinaryNameMapper {

        @Override
        public String typeName(String clsName) {
            return clsName;
        }

        @Override
        public String fieldName(String fieldName) {
            return fieldName;
        }
    }

    private static class MyBinaryIdMapper implements BinaryIdMapper {

        @Override
        public int typeId(String typeName) {
            return typeName.hashCode();
        }

        @Override
        public int fieldId(int typeId, String fieldName) {
            return typeId + fieldName.hashCode();
        }
    }

    private static class ExampleSerializer implements BinarySerializer {

        @Override
        public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {

        }

        @Override
        public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {

        }
    }
}
