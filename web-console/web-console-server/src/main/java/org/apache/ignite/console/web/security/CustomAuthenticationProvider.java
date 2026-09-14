

package org.apache.ignite.console.web.security;

import java.time.LocalDateTime;
import java.util.UUID;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.AccountsService;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.util.Assert;

import static java.time.temporal.ChronoUnit.MILLIS;

/**
 * Custom activation provider.
 */
public class CustomAuthenticationProvider extends DaoAuthenticationProvider {
    /** Timeout between emails with new activation token. */
    private long activationTimeout;

    /**
     * @param activationTimeout Activation timeout.
     */
    CustomAuthenticationProvider(long activationTimeout) {
        this.activationTimeout = activationTimeout;
    }

    /**
     * Validate activationToken token.
     *
     * @param acc Account object.
     * @param activationTok activate account token
     * @throws AuthenticationException if validation fails.
     */
    private void checkActivationToken(Account acc, UUID activationTok) throws AuthenticationException {
        if (acc.isEnabled()) {
            if (activationTok != null && activationTok.equals(acc.getActivationToken()))
                throw new BadCredentialsException(messages.getMessage(
                    "AbstractUserDetailsAuthenticationProvider.badCredentials",
                    "Bad credentials"));
        }
        else {
            if (activationTok == null)
                throw new MissingConfirmRegistrationException(messages.getMessage(
                    "AbstractUserDetailsAuthenticationProvider.missingActivationToken",
                    "Invalid or missing activation token"), acc.getEmail());

            if (!activationTok.equals(acc.getActivationToken()))
                throw new BadCredentialsException(messages.getMessage(
                    "AbstractUserDetailsAuthenticationProvider.badActivationToken",
                    "This activation token isn't valid."));
        }
    }

    /** {@inheritDoc} */
    @Override public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        Assert.isInstanceOf(UsernamePasswordAuthenticationToken.class, authentication,
            messages.getMessage("AbstractUserDetailsAuthenticationProvider.onlySupports",
                "Only UsernamePasswordAuthenticationToken is supported"));

        Assert.isInstanceOf(AccountsService.class, getUserDetailsService(),
            messages.getMessage("CustomAuthenticationProvider.onlySupports",
                "Only AccountsService is supported"));

        // Determine username
        String username = (authentication.getPrincipal() == null) ? "NONE_PROVIDED" : authentication.getName();

        UserDetails user = getUserCache().getUserFromCache(username);

        if (user == null) {
            try {
                user = retrieveUser(username, (UsernamePasswordAuthenticationToken)authentication);

                Assert.notNull(user,
                    "retrieveUser returned null - a violation of the interface contract");

                getUserCache().putUserInCache(user);
            }
            catch (UsernameNotFoundException notFound) {
                logger.debug("User '" + username + "' not found");

                if (hideUserNotFoundExceptions) {
                    throw new BadCredentialsException(messages.getMessage(
                        "AbstractUserDetailsAuthenticationProvider.badCredentials", "Bad credentials"));
                }
                else
                    throw notFound;
            }
        }

        if (user instanceof Account) {
            Account acc = (Account)user;

            UUID activationTok = null;

            if (authentication.getDetails() instanceof UUID)
                activationTok = (UUID)authentication.getDetails();

            checkActivationToken(acc, activationTok);

            AccountsService accountsSrv = (AccountsService)getUserDetailsService();

            if (!acc.isEnabled() &&
                MILLIS.between(acc.getActivationSentAt(), LocalDateTime.now()) >= activationTimeout) {
                accountsSrv.resetActivationToken(username);

                throw new MissingConfirmRegistrationException(messages.getMessage(
                    "AbstractUserDetailsAuthenticationProvider.expiredActivationToken",
                    "This activation link was expired. We resend a new one. Please open the most recent email and click on the activation link."),
                    acc.getEmail());
            }

            accountsSrv.activateAccount(acc.getId());
        }

        return super.authenticate(authentication);
    }
    
    /**
     * 判断只有传入UserAuthenticationToken的时候才使用这个Provider
     * supports会在AuthenticationManager层被调用
     * @param authentication
     * @return
     */
    public boolean supports(Class<?> authentication) {
        return UsernamePasswordAuthenticationToken.class.isAssignableFrom(authentication);
    }
}
