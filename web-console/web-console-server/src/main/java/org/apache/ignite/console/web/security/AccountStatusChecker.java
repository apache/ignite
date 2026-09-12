

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
