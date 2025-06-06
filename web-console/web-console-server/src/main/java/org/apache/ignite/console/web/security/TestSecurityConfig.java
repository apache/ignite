

package org.apache.ignite.console.web.security;

import org.apache.ignite.console.common.Test;
import org.apache.ignite.console.config.ActivationConfiguration;
import org.apache.ignite.console.services.AccountsService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Security settings provider for test profile.
 */
@Test
@Configuration
public class TestSecurityConfig extends SecurityConfig {
    /** Test path. */
    private static final String TEST_PATH = "/api/v1/test/";

    /**
     * @param activationCfg Account activation configuration.
     * @param encoder Service for encoding user passwords.
     * @param accountsSrv User details service.
     */
    public TestSecurityConfig(ActivationConfiguration activationCfg,
        PasswordEncoder encoder,
        AccountsService accountsSrv
    ) {
        super(activationCfg, encoder, accountsSrv);
    }

    /** {@inheritDoc} */
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http.authorizeRequests().requestMatchers(TEST_PATH).anonymous();

        super.securityFilterChain(http);

        http = http.csrf().disable();
        
        return http.build();
    }
}
