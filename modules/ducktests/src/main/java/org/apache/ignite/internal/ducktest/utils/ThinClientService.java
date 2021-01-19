package org.apache.ignite.internal.ducktest.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


import java.util.Base64;

public class ThinClientService {

    /** Logger. */
    private static final Logger log = LogManager.getLogger(ThinClientService.class.getName());

    /**
     * @param args Args.
     */

    public static void main(String[] args) throws Exception {
        log.info("Starting Application... [params=" + args[0] + "]");

        String[] params = args[0].split(",");

        boolean startIgnite = Boolean.parseBoolean(params[0]);

        Class<?> clazz = Class.forName(params[1]);

        String cfgPath = params[2];

        ObjectMapper mapper = new ObjectMapper();

        JsonNode jsonNode = params.length > 3 ?
                mapper.readTree(Base64.getDecoder().decode(params[3])) : mapper.createObjectNode();

        String host = jsonNode.get("server_address").asText();
        String port = jsonNode.get("port").asText();

        ThinkClientApplication app =
                (ThinkClientApplication)clazz.getConstructor().newInstance();

        app.cfgPath = cfgPath;

        if (startIgnite) {
            log.info("Starting Ignite node...");

            ClientConfiguration cfg = new ClientConfiguration().setAddresses(host + ":" + port);

            try (IgniteClient client = Ignition.startClient(cfg)) {
                app.client = client;

                app.start(jsonNode);
            }
            finally {
                log.info("Ignite instance closed. [interrupted=" + Thread.currentThread().isInterrupted() + "]");
            }
        }
        else
            app.start(jsonNode);
    }

}
