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
package org.apache.ignite.internal.util;

/**
 * Class extracted for fields from GridUnsafe to be absolutely independent with current and future static block
 * initialization effects.
 */
public class FeatureChecker {
    /** Required Options to Run on Java 9, 10, 11. */
    public static final String JAVA_9_10_11_OPTIONS = "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED\n" +
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED\n" +
        "--add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED\n" +
        "--add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED\n" +
        "--add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED\n" +
        "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED\n" +
        "--illegal-access=permit";

    /** Java version specific warning to be added in case access failed */
    public static final String JAVA_VER_SPECIFIC_WARN =
        "\nPlease add the following parameters to JVM startup settings and restart the application: {parameters: " +
            JAVA_9_10_11_OPTIONS +
            "\n}" +
            "\nSee https://apacheignite.readme.io/docs/getting-started#section-running-ignite-with-java-9-10-11 for more information.";
}
