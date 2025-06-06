

package org.apache.ignite.console;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * Grid config.
 */
@TestConfiguration
public class TestGridConfiguration {
    /**
     * @return Service for encoding user passwords.
     */
    @Bean
    @Primary
    public PasswordEncoder passwordEncoder() {
        return NoOpPasswordEncoder.getInstance();
    }

    /**
     * We overriding ignite creation bean for cases where the application context
     * needs to be recreated with the already running ignite instance.
     *
     * @param cfg Grid configuration.
     */
    @Primary
    @Bean(destroyMethod = "close")
    public IgniteEx igniteInstance(@Autowired IgniteConfiguration cfg) throws IgniteCheckedException {
        IgniteEx ignite = (IgniteEx) IgnitionEx.start(cfg, false);

        ignite.cluster().active(true);

        return ignite;
    }
}
