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

package org.apache.ignite.internal.multijvm;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Run ignite node. 
 */
public class IgniteNodeRunner {
    public static final String TASK_EXECUTED = "Event.TaskExecuted";
    public static final String EXECUTE_TASK = "Cmd.ExecuteTask=";
    public static final String TASK_ARGS = ", Cmd.TaskArgs=";
    public static final String STOP = "Cmd.Stop";

    public static void main(String[] args) throws Exception {
        try {
            X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

            assert args != null;
            assert args.length >= 1;

            X.println("Starting Ignite Node...");

            String cfg = args[0];

            Ignite ignite = Ignition.start(cfg);

            // Read commands.
            String cmd;

            final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            boolean running = true;

            while (running) {
                cmd = br.readLine();
    
                X.println(">>>>> Got cmd [" + cmd + ']');
    
                if (cmd.startsWith(EXECUTE_TASK)) {
                    String taskClsName = cmd.substring(EXECUTE_TASK.length(), cmd.indexOf(TASK_ARGS));
                    String taskArgs = cmd.substring(cmd.indexOf(TASK_ARGS) + TASK_ARGS.length());
    
                    X.println(">>>>> Task=" + taskClsName + ", args=" + taskArgs);
    
                    Task task = (Task)Class.forName(taskClsName).newInstance();
                    
                    task.execute(ignite, taskArgs.split(" "));
    
                    X.println(TASK_EXECUTED);
                }
                else if (cmd.startsWith(STOP)) {
                    Ignition.stopAll(false);
    
                    running = false;
                }
            }
        }
        catch (Throwable e) {
            e.printStackTrace();
            
            System.exit(1);
        }
    }
    
    public static interface Task {
        boolean execute(Ignite ignite, String... args);
    }
}
