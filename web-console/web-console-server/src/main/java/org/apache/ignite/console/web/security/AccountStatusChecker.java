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

import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.security.authentication.AccountExpiredException;
import org.springframework.security.authentication.CredentialsExpiredException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.core.SpringSecurityMessageSource;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsChecker;

/**
 * Checker for status of the loaded <tt>UserDetails</tt> object.
 */
public class AccountStatusChecker implements UserDetailsChecker {
    /** Messages accessor. */
    private final MessageSourceAccessor messages = SpringSecurityMessageSource.getAccessor();

    /** Activation enabled. */
    private boolean enabled;

    /**
     * @param enabled Activation enabled.
     */
    public AccountStatusChecker(boolean enabled) {
        this.enabled = enabled;
    }

    /** {@inheritDoc} */
    @Override public void check(UserDetails user) {
        if (!user.isAccountNonLocked()) {
            throw new LockedException(messages.getMessage(
                "AccountStatusUserDetailsChecker.locked", "User account is locked"));
        }

        if (enabled && !user.isEnabled()) {
            throw new MissingConfirmRegistrationException(messages.getMessage(
                "AccountStatusUserDetailsChecker.disabled", "User is disabled"), user.getUsername());
        }

        if (!user.isAccountNonExpired()) {
            throw new AccountExpiredException(
                messages.getMessage("AccountStatusUserDetailsChecker.expired", "User account has expired"));
        }

        if (!user.isCredentialsNonExpired()) {
            throw new CredentialsExpiredException(messages.getMessage(
                "AccountStatusUserDetailsChecker.credentialsExpired", "User credentials have expired"));
        }
    }
}
