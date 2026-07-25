

package org.apache.ignite.console.config;

import org.apache.ignite.console.agent.service.IMailService;
import org.apache.ignite.console.services.NoopMailService;
import org.apache.ignite.console.web.security.AccountStatusChecker;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.userdetails.UserDetailsChecker;

/**
 * Account activation configuration.
 */
@Configuration
@ConfigurationProperties("account.activation")
public class ActivationConfiguration implements InitializingBean {
    /** */
    private static final Logger log = LoggerFactory.getLogger(ActivationConfiguration.class);

    /** By default activation link will be available for 24 hours. */
    private static final long DFLT_TIMEOUT = 24 * 60 * 60 * 1000;

    /** By default activation send email throttle is 3 minutes. */
    private static final long DFLT_SEND_TIMEOUT = 3 * 60 * 1000;
    
    /** Is noop mail service. */
    private final boolean isNoopMailSrv;

    /** Whether account should be activated by e-mail confirmation. */
    private boolean enabled;

    /** Activation link life time. */
    private long timeout = DFLT_TIMEOUT;

    /** Activation send email throttle. */
    private long sndTimeout = DFLT_SEND_TIMEOUT;

    /** Service to check user status. */
    private UserDetailsChecker checker;

    /**
     * @param srv Mail sending service.
     */
    public ActivationConfiguration(IMailService srv) {
        isNoopMailSrv = srv instanceof NoopMailService;
    }

    /** {@inheritDoc} */
    @Override public void afterPropertiesSet() {
        if (enabled && isNoopMailSrv) {
            enabled = false;

            log.warn("Mail server settings are required for account confirmation.");
        }

        checker = new AccountStatusChecker(enabled);
    }

    /**
     * @return {@code true} if new accounts should be activated via e-mail confirmation.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled Accounts activation required flag.
     * @return {@code this} for chaining.
     */
    public ActivationConfiguration setEnabled(boolean enabled) {
        this.enabled = enabled;

        return this;
    }

    /**
     * @return Activation link life time.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     *
     * @param timeout Activation link life time.
     * @return {@code this} for chaining.
     */
    public ActivationConfiguration setTimeout(long timeout) {
        this.timeout = timeout;

        return this;
    }

    /**
     * @return Activation send email throttle.
     */
    public long getSendTimeout() {
        return sndTimeout;
    }

    /**
     * @param sndTimeout Activation send email throttle.
     * @return {@code this} for chaining.
     */
    public ActivationConfiguration setSendTimeout(long sndTimeout) {
        this.sndTimeout = sndTimeout;

        return this;
    }

    /**
     * @return Checker for status of the loaded <tt>UserDetails</tt> object.
     */
    public UserDetailsChecker getChecker() {
        return checker;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ActivationConfiguration.class, this);
    }
}
