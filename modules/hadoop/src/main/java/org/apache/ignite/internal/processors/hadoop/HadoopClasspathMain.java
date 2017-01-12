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

package org.apache.ignite.internal.processors.hadoop;

/**
 * Main class to compose Hadoop classpath depending on the environment.
 * This class is designed to be independent on any Ignite classes as possible.
 * Please make sure to pass the path separator character as the 1st parameter to the main method.
 */
public class HadoopClasspathMain {
    /**
     * Main method to be executed from scripts. It prints the classpath to the standard output.
     *
     * @param args The 1st argument should be the path separator character (":" on Linux, ";" on Windows).
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1)
            throw new IllegalArgumentException("Path separator must be passed as the first argument.");

        String separator = args[0];

        StringBuilder sb = new StringBuilder();

        for (String path : HadoopClasspathUtils.classpathForJavaProcess())
            sb.append(path).append(separator);

        System.out.println(sb);
    }
}
