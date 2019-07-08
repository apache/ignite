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
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.web.model.SignInRequest;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedCredentialsNotFoundException;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

/**
 * Custom filter for retrieve credentials from body and authenticate user. Default implementation use path parameters.
 */
public class BodyReaderAuthenticationFilter extends UsernamePasswordAuthenticationFilter {
    /** */
    protected ObjectMapper objMapper = new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** Messages accessor. */
    protected final MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /** {@inheritDoc} */
    @Override public Authentication attemptAuthentication(HttpServletRequest req,
        HttpServletResponse res) throws AuthenticationException {
        try {
            SignInRequest params = objMapper.readValue(req.getReader(), SignInRequest.class);

            UsernamePasswordAuthenticationToken tok =
                new UsernamePasswordAuthenticationToken(params.getEmail(), params.getPassword());

            // Configure auth token.
            setDetails(req, tok);

            tok.setDetails(params.getActivationToken());

            return getAuthenticationManager().authenticate(tok);
        }
        catch (IOException e) {
            throw new PreAuthenticatedCredentialsNotFoundException(messages.getMessage("err.parse-signin-req-failed"), e);
        }
    }
}
