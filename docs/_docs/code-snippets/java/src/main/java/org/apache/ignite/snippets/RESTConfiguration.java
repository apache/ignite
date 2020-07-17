package org.apache.ignite.snippets;

import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class RESTConfiguration {

    void config() {
        //tag::http-configuration[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setConnectorConfiguration(new ConnectorConfiguration().setJettyPath("jetty.xml"));
        //end::http-configuration[]
    }

}
