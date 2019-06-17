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

package org.apache.ignite.console.config;

import java.util.Map;
import org.apache.ignite.console.web.security.PassportLocalPasswordEncoder;
import org.apache.ignite.internal.util.typedef.F;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * Application configuration.
 **/
@Configuration
@EnableScheduling
public class ApplicationConfiguration {
    /**
     * @return Service for encoding user passwords.
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        String encodingId = "bcrypt";

        Map<String, PasswordEncoder> encoders = F.asMap(
            encodingId, new BCryptPasswordEncoder(),
            "pbkdf2", new PassportLocalPasswordEncoder()
        );

        return new DelegatingPasswordEncoder(encodingId, encoders);
    }
}
