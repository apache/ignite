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

package org.apache.ignite.tests.load;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.tests.pojos.Person;

/**
 * Implementation of {@link Generator} generating {@link Person} instance.
 */
public class PersonGenerator implements Generator {
    /** */
    private static final Date DATE = new Date();

    /** */
    private static final List<String> PHONES = new LinkedList<String>() {{
        add("1234567");
        add("7654321");
        add("1289054");
    }};

    /** {@inheritDoc} */
    @Override public Object generate(long i) {
        return new Person(i, Long.toString(i), Long.toString(i), (short)(i % 100), i % 2 == 0, i, i, DATE, PHONES);
    }
}
