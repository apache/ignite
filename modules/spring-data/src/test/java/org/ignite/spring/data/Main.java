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

package org.ignite.spring.data;

import java.util.List;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {
    public static void main(String args[]) {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();

        ctx.scan("org.ignite.spring.data");
        ctx.refresh();

        FirstRepository repo = ctx.getBean(FirstRepository.class);

        for (int i = 0; i < 100; i++)
            repo.save(i, new Person("person" + Integer.toHexString(i)));

        Service svc = ctx.getBean(Service.class);
        Iterable<Person> call = svc.call();

        for (Person person : call)
            System.out.println(person.getFirstName());

        List<Person> persons = repo.byVal("person4a");

        for (Person person : persons)
            System.out.println("query = " + person);

        List<Person> val = repo.findByFirstName("person4e");

        for (Person person : val)
            System.out.println("simple = " + person);

        List<Person> containing = repo.findByFirstNameContaining("person4");

        for (Person person : containing)
            System.out.println("containing = " + person);

        List<Person> top = repo.findTopByFirstNameContaining("person4");

        for (Person person : top)
            System.out.println("top = " + person);

        List<Person> like = repo.findFirst10ByFirstNameLike("person");

        for (Person person : like)
            System.out.println("like10 = " + person);


    }
}
