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
package org.apache.ignite.snippets;

import java.util.Arrays;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.deployment.uri.UriDeploymentSpi;
import org.junit.jupiter.api.Test;

public class UserCodeDeployment {

    @Test
    void fromUrl() {
        //tag::from-url[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        UriDeploymentSpi deploymentSpi = new UriDeploymentSpi();

        deploymentSpi.setUriList(Arrays
                .asList("http://username:password;freq=10000@www.mysite.com:110/ignite/user_libs"));

        cfg.setDeploymentSpi(deploymentSpi);

        try (Ignite ignite = Ignition.start(cfg)) {
            //execute the task represented by a class located in the "user_libs" url 
            ignite.compute().execute("org.mycompany.HelloWorldTask", "My Args");
        }
        //end::from-url[]
    }

    @Test
    void fromDirectory() {
        //tag::from-local-dir[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        UriDeploymentSpi deploymentSpi = new UriDeploymentSpi();

        deploymentSpi.setUriList(Arrays.asList("file://freq=2000@localhost/home/username/user_libs"));

        cfg.setDeploymentSpi(deploymentSpi);

        try (Ignite ignite = Ignition.start(cfg)) {
            //execute the task represented by a class located in the "user_libs" directory 
            ignite.compute().execute("org.mycompany.HelloWorldTask", "My Args");
        }
        //end::from-local-dir[]
    }

}
