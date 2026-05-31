

package org.apache.ignite.console.config;

import java.util.Collections;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

/**
 * Grid instance configuration.
 */
@Configuration
@ImportResource("classpath:ignite-config.xml")
public class GridConfiguration {
    /**
     * @param cfg Grid configuration.
     */
    @Bean(destroyMethod = "close")
    public IgniteEx igniteInstance(@Autowired IgniteConfiguration cfg) {
        IgniteEx ignite = (IgniteEx)Ignition.start(cfg);

        ignite.cluster().active(true);

        return ignite;
    }
}
