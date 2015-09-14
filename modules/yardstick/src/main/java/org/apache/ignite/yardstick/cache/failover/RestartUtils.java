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

package org.apache.ignite.yardstick.cache.failover;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Restart Utils.
 */
public final class RestartUtils {
    /**
     * Private default constructor.
     */
    private RestartUtils() {
        // No-op.
    }

    /**
     * @param remoteUser
     * @param hostName
     */
    public static String kill9(final String remoteUser, String hostName) {
        try {
            String line;

            String cmd = "ssh -o PasswordAuthentication=no ashutak@localhost \"pkill -9 -f 'Dyardstick.server*'\"";

            StringBuilder sb = new StringBuilder(">>> Executing command:"+'\n').append(cmd).append('\n');

            Process p = Runtime.getRuntime().exec(cmd);

            try(BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                sb.append("OUT>>").append('\n');

                while ((line = input.readLine()) != null)
                    sb.append(line).append('\n');
            }

            try(BufferedReader input = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
                sb.append("Err>>").append('\n');

                while ((line = input.readLine()) != null)
                    sb.append(line).append('\n');
            }

            p.waitFor();

            return sb.toString();
        }
        catch (Exception err) {
            err.printStackTrace();

            return err.toString();
        }
    }

    public static void start() {
        // TODO: implement.
    }
}
