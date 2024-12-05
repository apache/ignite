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

import org.apache.ignite.internal.util.typedef.internal.S;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Sign up configuration.
 **/
@Configuration
@ConfigurationProperties("account.signup")
public class SignUpConfiguration {
    /** Flag if self sign up enabled. */
    private boolean enabled = true;

    /**
     * @return {@code false} if sign up disabled and new accounts can be created only by administrator.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled {@code false} if signup disabled and new accounts can be created only by administrator.
     * @return {@code this} for chaining.
     */
    public SignUpConfiguration setEnabled(boolean enabled) {
        this.enabled = enabled;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SignUpConfiguration.class, this);
    }
}
