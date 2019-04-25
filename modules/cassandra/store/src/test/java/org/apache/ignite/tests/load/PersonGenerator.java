/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    private static final List<String> PHONES = new LinkedList<String>(){{
        add("1234567");
        add("7654321");
        add("1289054");
    }};

    /** {@inheritDoc} */
    @Override public Object generate(long i) {
        return new Person(i, Long.toString(i), Long.toString(i), (short)(i % 100), i % 2 == 0, i, i, DATE, PHONES);
    }
}
