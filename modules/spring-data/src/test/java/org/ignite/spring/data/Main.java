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
            repo.save(i, new Pojo(Integer.toHexString(i)));

        Service svc = ctx.getBean(Service.class);
        Iterable<Pojo> call = svc.call();

        for (Pojo pojo : call)
            System.out.println(pojo.getVal());

        List<Pojo> val = repo.findByVal("4e");

        for (Pojo pojo : val)
            System.out.println("pojo = " + pojo);

        List<Pojo> pojos = repo.byVal("4a");

        for (Pojo pojo : pojos)
            System.out.println("pojo = " + pojo);
    }
}
