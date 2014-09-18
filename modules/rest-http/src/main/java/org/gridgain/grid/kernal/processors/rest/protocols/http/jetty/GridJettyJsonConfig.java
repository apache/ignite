/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.protocols.http.jetty;

import net.sf.json.*;
import net.sf.json.processors.*;

import java.util.*;

/**
 * Jetty protocol json configuration.
 */
public class GridJettyJsonConfig extends JsonConfig {
    /**
     * Constructs default jetty json config.
     */
    public GridJettyJsonConfig() {
        registerJsonValueProcessor(UUID.class, new ToStringJsonProcessor());
    }

    /**
     * Helper class for simple to-string conversion for the beans.
     */
    private static class ToStringJsonProcessor implements JsonValueProcessor {
        /** {@inheritDoc} */
        @Override public Object processArrayValue(Object val, JsonConfig jsonCfg) {
            throw new UnsupportedOperationException("Serialize array to string is not supported: " + val);
        }

        /** {@inheritDoc} */
        @Override public Object processObjectValue(String key, Object val, JsonConfig jsonCfg) {
            return val == null ? null : val.toString();
        }
    }
}
