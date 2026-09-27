

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
