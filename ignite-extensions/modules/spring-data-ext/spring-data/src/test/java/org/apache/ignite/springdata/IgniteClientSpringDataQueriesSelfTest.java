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

import org.apache.ignite.springdata.misc.IgniteClientApplicationConfiguration;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonRepository;
import org.apache.ignite.springdata.misc.PersonRepositoryOtherIgniteInstance;
import org.apache.ignite.springdata.misc.PersonSecondRepository;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/** Tests Spring Data query operations when thin client is used for accessing the Ignite cluster. */
public class IgniteClientSpringDataQueriesSelfTest extends IgniteSpringDataQueriesSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ctx = new AnnotationConfigApplicationContext();

        ctx.register(IgniteClientApplicationConfiguration.class);
        ctx.refresh();

        repo = ctx.getBean(PersonRepository.class);
        repo2 = ctx.getBean(PersonSecondRepository.class);
        repoTWO = ctx.getBean(PersonRepositoryOtherIgniteInstance.class);

        for (int i = 0; i < CACHE_SIZE; i++) {
            repo.save(i, new Person("person" + Integer.toHexString(i),
                "lastName" + Integer.toHexString((i + 16) % 256)));
            repoTWO.save(i, new Person("TWOperson" + Integer.toHexString(i),
                "lastName" + Integer.toHexString((i + 16) % 256)));
        }
    }
}
