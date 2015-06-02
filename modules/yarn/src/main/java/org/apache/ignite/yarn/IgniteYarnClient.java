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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.*;
import org.apache.hadoop.yarn.conf.*;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.*;

import static org.apache.hadoop.yarn.api.ApplicationConstants.*;

/**
 * Ignite yarn client.
 */
public class IgniteYarnClient {
    /** */
    public static final Logger log = Logger.getLogger(IgniteYarnClient.class.getSimpleName());

    /**
     * Main methods has only one optional parameter - path to properties files.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // Create application via yarnClient
        YarnClientApplication app = yarnClient.createApplication();

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        amContainer.setCommands(
                Collections.singletonList(
                        " $JAVA_HOME/bin/java -Xmx256M org.apache.ignite.yarn.ApplicationMaster" +
                        " 1>" + LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + LOG_DIR_EXPANSION_VAR + "/stderr"
                )
        );

        // Setup jar for ApplicationMaster
        final LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        setupAppMasterJar(new Path("/user/ntikhonov/ignite-yarn.jar"), appMasterJar, conf);

        final LocalResource igniteZip = Records.newRecord(LocalResource.class);
        setupAppMasterJar(new Path("/user/ntikhonov/gridgain-community-fabric-1.0.6.zip"), igniteZip, conf);

        FileSystem fileSystem = FileSystem.get(conf);

        Path path = fileSystem.makeQualified(new Path("/user/ntikhonov/gridgain-community-fabric-1.0.6/bin/ignite.sh"));

        System.out.println("Path: " + path);
        System.out.println("Path URI: " + path.toUri().toString());

        amContainer.setLocalResources(new HashMap<String, LocalResource>(){{
            put("ignite-yarn.jar", appMasterJar);
            put("ignite", igniteZip);
        }});

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv, conf);
        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext =
                app.getApplicationSubmissionContext();
        appContext.setApplicationName("simple-yarn-app"); // application name
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default"); // queue

        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        yarnClient.submitApplication(appContext);

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        System.out.println(
                "Application " + appId + " finished with" +
                        " state " + appState +
                        " at " + appReport.getFinishTime());
    }

    private static void setupAppMasterJar(Path jarPath, LocalResource appMasterJar, YarnConfiguration conf)
        throws Exception {
        FileSystem fileSystem = FileSystem.get(conf);
        jarPath = fileSystem.makeQualified(jarPath);

        FileStatus jarStat = fileSystem.getFileStatus(jarPath);

        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.ARCHIVE);
        appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);

        System.out.println("Path :" + jarPath);
    }

    private static void setupAppMasterEnv(Map<String, String> appMasterEnv, YarnConfiguration conf) {
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH))
            Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
                    c.trim(), File.pathSeparator);

        Apps.addToEnvironment(appMasterEnv,
                Environment.CLASSPATH.name(),
                Environment.PWD.$() + File.separator + "*",
                File.pathSeparator);
    }
}
