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

package org.apache.ignite.util.mbeans;

import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_MBEANS;

/**
 * MBean test.
 */
public class GridMBeanSelfTest  extends GridCommonAbstractTest implements IgniteInClosure<String> {
    static String policyFileName = GridMBeanSelfTestMain.policyFileName;
    static String propId = "java.security.policy";

    public void testInst() throws Exception {
        List<String> jvmArgs = Arrays.asList("-D"+IGNITE_DISABLE_MBEANS+"=true",
                "-Djava.security.manager", "-D" + propId + "=" + policyFileName);

        GridJavaProcess proc = null;

        try {
            proc = GridJavaProcess.exec(
                    GridMBeanSelfTestMain.class,
                    null, // Params.
                    this.log,
                    // Optional closure to be called each time wrapped process prints line to system.out or system.err.
                    this,
                    null,
                    jvmArgs,
                    null
            );

            Process process = proc.getProcess();

            int timeout = 300;

            boolean finished = process.waitFor(timeout, TimeUnit.SECONDS);

            if (!finished) {
                proc.kill();
                System.out.println("process timeout:" + timeout + " seconds");
            }

            int exitCode = process.waitFor();

            assertEquals("Unexpected exit code", 0, exitCode);
        }
        finally {
            if (proc != null)
                proc.killProcess();

        }
    }

    @Override
    public void apply(String s) {

        System.out.print("(proc)");

        System.out.println(s);
    }
}
