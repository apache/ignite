/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.web.security;

import org.apache.ignite.console.common.Test;
import org.apache.ignite.console.config.ActivationConfiguration;
import org.apache.ignite.console.services.AccountsService;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * Security settings provider for test profile.
 */
@Test
@Configuration
public class TestSecurityConfig extends SecurityConfig {
    /** Test path. */
    private static final String TEST_PATH = "/api/v1/test/**";

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
    @Override protected void configure(HttpSecurity http) throws Exception {
        http .authorizeRequests().antMatchers(TEST_PATH).anonymous();

        super.configure(http);

        http.csrf().disable();
    }
}
