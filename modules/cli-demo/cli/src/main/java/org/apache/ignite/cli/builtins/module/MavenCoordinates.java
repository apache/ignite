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

package org.apache.ignite.cli.builtins.module;

import org.apache.ignite.cli.IgniteCLIException;

public class MavenCoordinates {
    public final String groupId;
    public final String artifactId;
    public final String version;

    public MavenCoordinates(String groupId, String artifactId, String version) {
        this.groupId = groupId;
        this.artifactId = artifactId;
        this.version = version;
    }

    static MavenCoordinates of(String mvnString) {
        String[] coords = mvnString.split(":");

        if (coords.length == 4)
            return new MavenCoordinates(coords[1], coords[2], coords[3]);
        else
            throw new IgniteCLIException("Incorrect maven coordinates " + mvnString);
    }

    static MavenCoordinates of(String mvnString, String version) {
        return of(mvnString + ":" + version);
    }
}
