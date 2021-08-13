/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.rest.presentation.hocon;

import java.util.List;
import com.typesafe.config.ConfigException;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.rest.presentation.ConfigurationPresentation;
import org.jetbrains.annotations.Nullable;

import static com.typesafe.config.ConfigFactory.parseString;
import static com.typesafe.config.ConfigRenderOptions.concise;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.split;

/**
 * Representing the configuration as a string using HOCON as a converter to JSON.
 */
public class HoconPresentation implements ConfigurationPresentation<String> {
    /** Configuration registry. */
    private final ConfigurationRegistry registry;

    /**
     * Constructor.
     *
     * @param registry Configuration registry.
     */
    public HoconPresentation(ConfigurationRegistry registry) {
        this.registry = registry;
    }

    /** {@inheritDoc} */
    @Override public String represent() {
        return representByPath(null);
    }

    /** {@inheritDoc} */
    @Override public String representByPath(@Nullable String path) {
        return HoconConverter.represent(registry, path == null ? List.of() : split(path)).render(concise());
    }

    /** {@inheritDoc} */
    @Override public void update(String cfgUpdate) {
        if (cfgUpdate.isBlank())
            throw new IllegalArgumentException("Empty configuration");

        try {
            registry.change(HoconConverter.hoconSource(parseString(cfgUpdate).root()), null).get();
        }
        catch (IllegalArgumentException | ConfigurationValidationException e) {
            throw e;
        }
        catch (ConfigException.Parse e) {
            throw new IllegalArgumentException(e);
        }
        catch (Throwable t) {
            RuntimeException e;

            if (t.getCause() instanceof IllegalArgumentException)
                e = (RuntimeException)t.getCause();
            else if (t.getCause() instanceof ConfigurationValidationException)
                e = (RuntimeException)t.getCause();
            else
                e = new IgniteException(t);

            throw e;
        }
    }
}
