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

package org.apache.ignite.testframework.junits.multijvm;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link JavaVersionCommandParser}.
 */
@RunWith(Parameterized.class)
public class JavaVersionCommandParserTest {
    /** 'java -version' command output. */
    @Parameterized.Parameter(0)
    public String commandOutput;

    /** Major version corresponding to the output. */
    @Parameterized.Parameter(1)
    public int majorVersion;

    /** Returns dataset for testing major version extraction. */
    @Parameterized.Parameters()
    public static Collection<Object[]> dataset() {
        return Arrays.asList(
            new Object[]{
                "java version \"1.8.0_311\"\n" +
                    "Java(TM) SE Runtime Environment (build 1.8.0_311-b11)\n" +
                    "Java HotSpot(TM) Server VM (build 25.311-b11, mixed mode)",
                8
            },
            new Object[] {
                "openjdk version \"1.8.0_352\"\n" +
                    "OpenJDK Runtime Environment (build 1.8.0_352-8u352-ga-1~22.04-b08)\n" +
                    "OpenJDK 64-Bit Server VM (build 25.352-b08, mixed mode)",
                8
            },
            new Object[]{
                "java version \"11.0.6\" 2020-01-14 LTS\n" +
                    "Java(TM) SE Runtime Environment 18.9 (build 11.0.6+8-LTS)\n" +
                    "Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.6+8-LTS, mixed mode)",
                11
            },
            new Object[]{
                "java version \"19\" 2022-09-20\n" +
                    "Java(TM) SE Runtime Environment (build 19+36-2238)\n" +
                    "Java HotSpot(TM) 64-Bit Server VM (build 19+36-2238, mixed mode, sharing)\n",
                19
            }
        );
    }

    /**
     * Tests major version extraction.
     */
    @Test
    public void extractsMajorVersion() {
        assertThat(JavaVersionCommandParser.extractMajorVersion(commandOutput), is(majorVersion));
    }
}
