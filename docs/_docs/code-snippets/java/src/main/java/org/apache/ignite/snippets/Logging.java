package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.jcl.JclLogger;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.logger.log4j2.Log4J2Logger;
import org.apache.ignite.logger.slf4j.Slf4jLogger;

public class Logging {

    void log4j() throws IgniteCheckedException {

        // tag::log4j[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        IgniteLogger log = new Log4JLogger("log4j-config.xml");

        cfg.setGridLogger(log);

        // Start a node.
        try (Ignite ignite = Ignition.start(cfg)) {
            ignite.log().info("Info Message Logged!");
        }
        // end::log4j[]
    }

    void log4j2() throws IgniteCheckedException {
        // tag::log4j2[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        IgniteLogger log = new Log4J2Logger("log4j2-config.xml");

        cfg.setGridLogger(log);

        // Start a node.
        try (Ignite ignite = Ignition.start(cfg)) {
            ignite.log().info("Info Message Logged!");
        }
        // end::log4j2[]
    }

    void jcl() {
        //tag::jcl[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridLogger(new JclLogger());

        // Start a node.
        try (Ignite ignite = Ignition.start(cfg)) {
            ignite.log().info("Info Message Logged!");
        }
        //end::jcl[]
    }
    
       void slf4j() {
        //tag::slf4j[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridLogger(new Slf4jLogger());

        // Start a node.
        try (Ignite ignite = Ignition.start(cfg)) {
            ignite.log().info("Info Message Logged!");
        }
        //end::slf4j[]
    }

    public static void main(String[] args) throws IgniteCheckedException {
        Logging logging = new Logging();

        logging.jcl();
        logging.slf4j();
    }
}
