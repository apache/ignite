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

package org.apache.ignite.springdata;

import java.util.List;
import javax.cache.Cache;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

public class Main {
    public static void main(String args[]) {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();

        ctx.scan("org.apache.ignite.springdata");
        ctx.refresh();

        FirstRepository repo = ctx.getBean(FirstRepository.class);

        for (int i = 0; i < 1000; i++)
            repo.save(i, new Person("person" + Integer.toHexString(i), "lastName" + Integer.toHexString(i+16)));

        Service svc = ctx.getBean(Service.class);
        Iterable<Person> call = svc.call();

        for (Person person : call)
            System.out.println(person.getFirstName());

        List<Person> persons = repo.byQuery("person4a");

        for (Person person : persons)
            System.out.println("query = " + person);

        List<Person> val = repo.findByFirstName("person4e");

        for (Person person : val)
            System.out.println("simple = " + person);

        List<Person> containing = repo.findByFirstNameContaining("person4");

        for (Person person : containing)
            System.out.println("containing = " + person);

        Iterable<Person> top = repo.findTopByFirstNameContaining("person4");

        for (Person person : top)
            System.out.println("top = " + person);

        Iterable<Person> like = repo.findFirst10ByFirstNameLike("person");

        for (Person person : like)
            System.out.println("like10 = " + person);

        int count = repo.countByFirstNameLike("person");

        System.out.println("count1 = " + count);

        count = repo.countByFirstNameLike("person4");
        System.out.println("count2 = " + count);

        PageRequest pageable = new PageRequest(1, 5, Sort.Direction.DESC, "firstName");

        List<Person> pageable1 = repo.findByFirstNameRegex("^[a-z]+$", pageable);
        for (Person person : pageable1)
            System.out.println("pageable1 = " + person);

        List<Person> pageable2 = repo.findByFirstNameRegex("^[a-z]+$", pageable.next());
        for (Person person : pageable2)
            System.out.println("pageable2 = " + person);

        int countAnd = repo.countByFirstNameLikeAndSecondNameLike("person1", "lastName1");
        System.out.println("countAnd = " + countAnd);

        int countOr = repo.countByFirstNameStartingWithOrSecondNameStartingWith("person1", "lastName1");
        System.out.println("countOr = " + countOr);

        List<Person> queryWithSort = repo.byQuery2("^[a-z]+$", new Sort(Sort.Direction.DESC, "secondName"));
        for (Person person : queryWithSort)
            System.out.println("queryWithSort = " + person);

        List<Person> queryWithPaging = repo.byQuery3("^[a-z]+$", new PageRequest(1, 7, Sort.Direction.DESC, "secondName"));
        for (Person person : queryWithPaging)
            System.out.println("queryWithPaging = " + person);

        List<String> queryFields = repo.byQuery4("^[a-z]+$", new PageRequest(1, 7, Sort.Direction.DESC, "secondName"));
        for (String queryField : queryFields)
            System.out.println("queryField = " + queryField);

        List<Cache.Entry<Integer, Person>> cacheEntries = repo.findBySecondNameLike("lastName1");
        for (Cache.Entry<Integer, Person> entry : cacheEntries)
            System.out.println("listEntries: key=" + entry.getKey() + ", value=" + entry.getValue());

        Cache.Entry<Integer, Person> cacheEntry = repo.findTopBySecondNameLike("lastName18");
        System.out.println("oneEntry: key=" + cacheEntry.getKey() + ", value=" + cacheEntry.getValue());

        Person person = repo.findTopBySecondNameStartingWith("lastName18");
        System.out.println("person = " + person);

        List<List> lists = repo.byQuery5("^[a-z]+$", new PageRequest(2, 6));
        for (List list : lists)
            System.out.println("listlist: key=" + list.get(0) + ", secondName=" + list.get(1));
    }
}
