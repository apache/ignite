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
