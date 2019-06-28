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

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.config.ActivationConfiguration;
import org.apache.ignite.console.services.AccountsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UserDetailsChecker;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.access.intercept.FilterSecurityInterceptor;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.authentication.logout.HttpStatusReturningLogoutSuccessHandler;
import org.springframework.security.web.authentication.switchuser.SwitchUserFilter;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.session.ExpiringSession;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;
import org.springframework.session.config.annotation.web.http.EnableSpringHttpSession;

import static org.apache.ignite.console.dto.Account.ROLE_ADMIN;
import static org.apache.ignite.console.dto.Account.ROLE_USER;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketEvents.BROWSERS_PATH;

/**
 * Security settings provider.
 */
@Configuration
@EnableWebSecurity
@EnableSpringHttpSession
@Profile("!test")
public class SecurityConfig extends WebSecurityConfigurerAdapter {
    /** The number of seconds that the {@link Session} should be kept alive between client requests. */
    private static final int MAX_INACTIVE_INTERVAL_SECONDS = 60 * 60 * 24 * 30;

    /** Sign in route. */
    public static final String SIGN_IN_ROUTE = "/api/v1/signin";

    /** Sign up route. */
    public static final String SIGN_UP_ROUTE = "/api/v1/signup";

    /** Logout route. */
    private static final String LOGOUT_ROUTE = "/api/v1/logout";

    /** Forgot password route. */
    private static final String FORGOT_PASSWORD_ROUTE = "/api/v1/password/forgot";

    /** Reset password route. */
    private static final String RESET_PASSWORD_ROUTE = "/api/v1/password/reset";

    /** Resend activation token. */
    private static final String ACTIVATION_RESEND = "/api/v1/activation/resend";

    /** Switch user url. */
    private static final String SWITCH_USER_URL = "/api/v1/admin/login/impersonate";

    /** Exit user url. */
    private static final String EXIT_USER_URL = "/api/v1/logout/impersonate";

    /** Public routes. */
    private static final String[] PUBLIC_ROUTES = new String[] {
        AGENTS_PATH,
        SIGN_IN_ROUTE, SIGN_UP_ROUTE,
        FORGOT_PASSWORD_ROUTE, RESET_PASSWORD_ROUTE, ACTIVATION_RESEND
    };

    /** */
    private final AccountsService accountsSrv;
    
    /** */
    private final PasswordEncoder encoder;

    /** */
    private UserDetailsChecker userDetailsChecker;

    /** Is account email should be confirmed. */
    private boolean activationEnabled;

    /** Timeout between emails with new activation token. */
    private long activationTimeout;

    /**
     * @param activationCfg Account activation configuration.
     * @param encoder Service for encoding user passwords.
     * @param accountsSrv User details service.
     */
    @Autowired
    public SecurityConfig(
        ActivationConfiguration activationCfg,
        PasswordEncoder encoder,
        AccountsService accountsSrv
    ) {
        userDetailsChecker = activationCfg.getChecker();
        activationEnabled = activationCfg.isEnabled();
        activationTimeout = activationCfg.getTimeout();

        this.encoder = encoder;
        this.accountsSrv = accountsSrv;
    }

    /** {@inheritDoc} */
    @Override protected void configure(HttpSecurity http) throws Exception {
        http
            .csrf()
            .csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
            .and()
            .authorizeRequests()
            .antMatchers(PUBLIC_ROUTES).anonymous()
            .antMatchers("/api/v1/admin/**").hasAuthority(ROLE_ADMIN)
            .antMatchers("/api/v1/**", BROWSERS_PATH).hasAuthority(ROLE_USER)
            .antMatchers(EXIT_USER_URL).authenticated()
            .and()
            .addFilterAt(authenticationFilter(), UsernamePasswordAuthenticationFilter.class)
            .addFilterAfter(switchUserFilter(), FilterSecurityInterceptor.class)
            .logout()
            .logoutUrl(LOGOUT_ROUTE)
            .deleteCookies("SESSION")
            .logoutSuccessHandler(new HttpStatusReturningLogoutSuccessHandler(HttpStatus.OK));
    }

    /** {@inheritDoc} */
    @Override public void configure(WebSecurity web) {
        web.ignoring().antMatchers(
            "/v2/api-docs",
            "/configuration/ui",
            "/swagger-resources",
            "/swagger-resources/configuration/ui",
            "/swagger-resources/configuration/security",
            "/configuration/security",
            "/swagger-ui.html",
            "/webjars/**"
        );
    }

    /**
     * Configure global implementation of {@link #authenticationManager()}
     */
    @Autowired
    public void configureGlobal(AuthenticationManagerBuilder auth) {
        DaoAuthenticationProvider authProvider = activationEnabled ?
            new CustomAuthenticationProvider(activationTimeout) : new DaoAuthenticationProvider();

        authProvider.setPreAuthenticationChecks(userDetailsChecker);
        authProvider.setUserDetailsService(accountsSrv);
        authProvider.setPasswordEncoder(encoder);

        auth.authenticationProvider(authProvider);
    }

    /**
     * @param req Request.
     * @param res Response.
     * @param authentication Authentication.
     */
    private void successHandler(
        HttpServletRequest req,
        HttpServletResponse res,
        Authentication authentication
    ) throws IOException {
        res.setStatus(HttpServletResponse.SC_OK);

        res.getWriter().flush();
    }

    /**
     * @param ignite Ignite.
     */
    @Bean
    public SessionRepository<ExpiringSession> sessionRepository(@Autowired Ignite ignite) {
        return new IgniteSessionRepository(ignite)
            .setDefaultMaxInactiveInterval(MAX_INACTIVE_INTERVAL_SECONDS);
    }

    /**
     * Custom filter for retrieve credentials.
     */
    private BodyReaderAuthenticationFilter authenticationFilter() throws Exception {
        BodyReaderAuthenticationFilter authenticationFilter = new BodyReaderAuthenticationFilter();

        authenticationFilter.setAuthenticationManager(authenticationManagerBean());
        authenticationFilter.setRequiresAuthenticationRequestMatcher(new AntPathRequestMatcher(SIGN_IN_ROUTE, "POST"));
        authenticationFilter.setAuthenticationSuccessHandler(this::successHandler);

        return authenticationFilter;
    }

    /**
     * Switch User processing filter.
     */
    public SwitchUserFilter switchUserFilter() {
        SwitchUserFilter filter = new SwitchUserFilter();

        filter.setUserDetailsService(accountsSrv);
        filter.setSuccessHandler(this::successHandler);
        filter.setUsernameParameter("email");
        filter.setSwitchUserUrl(SWITCH_USER_URL);
        filter.setExitUserUrl(EXIT_USER_URL);

        return filter;
    }
}
