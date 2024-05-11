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

import org.springframework.boot.web.server.WebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;

/**
 * Programmatic configuration of default server port.
 */
@Component
public class ServerPortCustomizer implements WebServerFactoryCustomizer<ConfigurableServletWebServerFactory>, Ordered {
    /** */
    private static final int DFLT_PORT = 3000;

    /** {@inheritDoc} */
    @Override public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }

	@Override
	public void customize(ConfigurableServletWebServerFactory factory) {
		
		factory.setPort(DFLT_PORT);
	}

	
}
