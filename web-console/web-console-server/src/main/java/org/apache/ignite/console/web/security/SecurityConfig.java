

package org.apache.ignite.console.web.security;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.annotation.PostConstruct;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.config.ActivationConfiguration;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.AccountsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.TaskDecorator;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfiguration;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UserDetailsChecker;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.FilterSecurityInterceptor;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.authentication.logout.HttpStatusReturningLogoutSuccessHandler;
import org.springframework.security.web.authentication.switchuser.SwitchUserFilter;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

import org.springframework.session.MapSession;
import org.springframework.session.MapSessionRepository;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;
import org.springframework.session.config.annotation.web.http.EnableSpringHttpSession;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import static org.apache.ignite.console.dto.Account.ROLE_ADMIN;
import static org.apache.ignite.console.dto.Account.ROLE_USER;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketEvents.BROWSERS_PATH;
import static org.springframework.security.config.Customizer.withDefaults;

/**
 * Security settings provider.
 */
@Configuration
@EnableWebSecurity
@EnableSpringHttpSession
@Profile("!test")
public class SecurityConfig {
    /** The number of seconds that the {@link Session} should be kept alive between client requests. */
    private static final int MAX_INACTIVE_INTERVAL_SECONDS = 60 * 60 * 24 * 30;

    /** Sign in route. */
    public static final String SIGN_IN_ROUTE = "/api/v1/signin";
    
    /** Login route. same as signin but return user info */
    public static final String LOGIN_ROUTE = "/api/v1/login";

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
        SIGN_IN_ROUTE, SIGN_UP_ROUTE, LOGIN_ROUTE,
        FORGOT_PASSWORD_ROUTE, RESET_PASSWORD_ROUTE, ACTIVATION_RESEND
    };

    final static Map<String, Session> sessions = new ConcurrentHashMap<>();

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

    @PostConstruct
    public void setSecurityContextStrategy() {
        SecurityContextHolder.setStrategyName(
                SecurityContextHolder.MODE_THREADLOCAL
        );
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(Arrays.asList("*"));
        configuration.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"));
        configuration.setAllowedHeaders(Arrays.asList("*"));
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/api/v1/**", configuration);
        return source;
    }

    /** {@inheritDoc} */
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
    	
    	//-http.csrf().csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse());

        http = http.cors(cors -> cors.configurationSource(corsConfigurationSource()));
    	
    	http = http.csrf(csrf -> csrf.disable());
		
		http.headers(headers -> headers
            // 禁用 X-Frame-Options
            .frameOptions(frameOptions -> frameOptions.disable())
            // 设置 CSP 允许特定域名 iframe 加载
            .contentSecurityPolicy(csp -> csp
                .policyDirectives("frame-ancestors 'self' http://localhost")
            )
        )
        
        http.authorizeHttpRequests(auth->auth
                .requestMatchers(PUBLIC_ROUTES).anonymous()
                .requestMatchers("/api/v1/admin/**").hasAuthority(ROLE_ADMIN)
                .requestMatchers("/api/v1/**", BROWSERS_PATH).hasAuthority(ROLE_USER)
                .requestMatchers(EXIT_USER_URL).authenticated().anyRequest().permitAll()
                )
            .addFilterAfter(switchUserFilter(), FilterSecurityInterceptor.class)
            .addFilterAt(authenticationTokenFilter(), FilterSecurityInterceptor.class)
            .logout(logout -> logout
                .logoutUrl(LOGOUT_ROUTE)
                .deleteCookies("SESSION")
                .logoutSuccessHandler(new HttpStatusReturningLogoutSuccessHandler(HttpStatus.OK))
            );

        /*
        http.sessionManagement(session ->
                session.sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED)
        );

        http.securityContext(context->
                context.securityContextRepository(new HttpSessionSecurityContextRepository())
        );

        http.rememberMe(rememberMe -> rememberMe
                .rememberMeCookieName("ignite-remember")
                .key("admin")
                .tokenValiditySeconds(3600 * 12));
        */
        return http.build();
    }

    /** {@inheritDoc} */
    @Bean
    public WebSecurityCustomizer webSecurityCustomizer() {
    	return (web) -> web.ignoring().requestMatchers(
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
        Account account = (Account)authentication.getPrincipal();
        res.addCookie(new Cookie("principal",account.getUsername()));
        //res.getWriter().flush();
    }

    /**
     * @param ignite Ignite.
     */
    @Bean
    public SessionRepository<MapSession> sessionRepository(@Autowired Ignite ignite) {
        if(true || !ignite.cluster().state().active())
            return new MapSessionRepository(sessions);
        return new IgniteSessionRepository(ignite)
            .setDefaultMaxInactiveInterval(MAX_INACTIVE_INTERVAL_SECONDS);
    }

    /**
     * Switch User processing filter.
     */
    @Bean
    public SwitchUserFilter switchUserFilter() {
        SwitchUserFilter filter = new SwitchUserFilter();

        filter.setUserDetailsService(accountsSrv);
        filter.setSuccessHandler(this::successHandler);
        filter.setUsernameParameter("email");
        filter.setSwitchUserUrl(SWITCH_USER_URL);
        filter.setExitUserUrl(EXIT_USER_URL);

        return filter;
    }
    

    /**
     * Switch User processing filter.
     */
    @Bean
    public AuthenticationTokenFilter authenticationTokenFilter() {
    	AuthenticationTokenFilter filter = new AuthenticationTokenFilter();
    	filter.setAccountsService(accountsSrv);
        return filter;
    }
    
    @Bean
    public AuthenticationManager authenticationManager() {
    	DaoAuthenticationProvider authProvider = activationEnabled ?
    	            new CustomAuthenticationProvider(activationTimeout) : new DaoAuthenticationProvider();

        authProvider.setPreAuthenticationChecks(userDetailsChecker);
        authProvider.setUserDetailsService(accountsSrv);
        authProvider.setPasswordEncoder(encoder);
        return new ProviderManager(authProvider);
    }

}
