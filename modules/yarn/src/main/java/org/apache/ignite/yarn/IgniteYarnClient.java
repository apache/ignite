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

package org.apache.ignite.yarn;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.*;
import org.apache.hadoop.yarn.conf.*;
import org.apache.hadoop.yarn.util.*;
import org.apache.ignite.yarn.utils.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import static org.apache.hadoop.yarn.api.ApplicationConstants.*;

/**
 * Ignite yarn client.
 */
public class IgniteYarnClient {
    /** */
    public static final Logger log = Logger.getLogger(IgniteYarnClient.class.getSimpleName());

    /**
     * Main methods has one mandatory parameter and one optional parameter.
     *
     * @param args Path to jar mandatory parameter and property file is optional.
     */
    public static void main(String[] args) throws Exception {
        checkArguments(args);

        // Set path to app master jar.
        String pathAppMasterJar = args[0];

        ClusterProperties props = ClusterProperties.from(args.length == 2 ? args[1] : null);

        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // Create application via yarnClient
        YarnClientApplication app = yarnClient.createApplication();

        FileSystem fs = FileSystem.get(conf);

        Path ignite;

        // Load ignite and jar
        if (props.ignitePath() == null)
            ignite = getIgnite(props, fs);
        else
            ignite = new Path(props.ignitePath());

        Path appJar = IgniteYarnUtils.copyLocalToHdfs(fs, pathAppMasterJar,
            props.igniteWorkDir() + File.separator + IgniteYarnUtils.JAR_NAME);

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        amContainer.setCommands(
            Collections.singletonList(
                Environment.JAVA_HOME.$() + "/bin/java -Xmx512m " + ApplicationMaster.class.getName()
                + IgniteYarnUtils.SPACE + ignite.toUri()
                + IgniteYarnUtils.YARN_LOG_OUT
            )
        );

        // Setup jar for ApplicationMaster
        LocalResource appMasterJar = IgniteYarnUtils.setupFile(appJar, fs, LocalResourceType.FILE);

        amContainer.setLocalResources(Collections.singletonMap(IgniteYarnUtils.JAR_NAME, appMasterJar));

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = props.toEnvs();

        setupAppMasterEnv(appMasterEnv, conf);

        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(512);
        capability.setVirtualCores(1);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName("ignition"); // application name
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default"); // queue

        // Submit application
        ApplicationId appId = appContext.getApplicationId();

        yarnClient.submitApplication(appContext);

        log.log(Level.INFO, "Submitted application. Application id: {0}", appId);

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();

        while (appState == YarnApplicationState.NEW ||
            appState == YarnApplicationState.NEW_SAVING ||
            appState == YarnApplicationState.SUBMITTED ||
            appState == YarnApplicationState.ACCEPTED) {
            TimeUnit.SECONDS.sleep(1L);

            appReport = yarnClient.getApplicationReport(appId);

            if (appState != YarnApplicationState.ACCEPTED
                && appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED)
                log.log(Level.INFO, "Application {0} is ACCEPTED.", appId);

            appState = appReport.getYarnApplicationState();
        }

        log.log(Level.INFO, "Application {0} is {1}.", new Object[]{appId, appState});
    }

    /**
     * Check input arguments.
     *
     * @param args Arguments.
     */
    private static void checkArguments(String[] args) {
        if (args.length < 1)
            throw new IllegalArgumentException("Invalid arguments.");
    }

    /**
     * @param props Properties.
     * @param fileSystem Hdfs file system.
     * @return Hdfs path to ignite node.
     * @throws Exception
     */
    private static Path getIgnite(ClusterProperties props, FileSystem fileSystem) throws Exception {
        IgniteProvider provider = new IgniteProvider(props, fileSystem);

        if (props.igniteUrl() == null)
            return provider.getIgnite();
        else
            return provider.getIgnite(props.igniteUrl());
    }

    /**
     * @param envs Environment variables.
     * @param conf Yarn configuration.
     */
    private static void setupAppMasterEnv(Map<String, String> envs, YarnConfiguration conf) {
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH))
            Apps.addToEnvironment(envs, Environment.CLASSPATH.name(),
                    c.trim(), File.pathSeparator);

        Apps.addToEnvironment(envs,
                Environment.CLASSPATH.name(),
                Environment.PWD.$() + File.separator + "*",
                File.pathSeparator);
    }
}
