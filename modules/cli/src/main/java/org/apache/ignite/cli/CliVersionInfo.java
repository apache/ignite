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

package org.apache.ignite.cli;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import javax.inject.Singleton;

/**
 * Provider of current Ignite CLI version info from the builtin properties file.
 */
@Singleton
public class CliVersionInfo {
    /** Ignite CLI version. */
    public final String ver;

    /**
     * Creates Ignite CLI version provider according to builtin version file.
     */
    public CliVersionInfo() {
        try (InputStream inputStream = CliVersionInfo.class.getResourceAsStream("/version.properties")) {
            Properties prop = new Properties();
            prop.load(inputStream);

            ver = prop.getProperty("version", "undefined");
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can' read ignite version info");
        }
    }

    /**
     * Creates Ignite CLI version provider from the manually setted version.
     * @param ver Ignite CLI version
     */
    public CliVersionInfo(String ver) {
        this.ver = ver;
    }
}
