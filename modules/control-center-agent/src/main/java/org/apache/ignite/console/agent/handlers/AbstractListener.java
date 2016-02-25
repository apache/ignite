/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.console.agent.handlers;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import io.socket.client.Ack;
import io.socket.emitter.Emitter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Base class for
 */
abstract class AbstractListener implements Emitter.Listener {
    /** JSON object mapper. */
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        JsonOrgModule module = new JsonOrgModule();

        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        mapper.registerModule(module);
    }

    /**
     * @param obj Object.
     * @return {@link JSONObject} or {@link JSONArray}.
     */
    private Object toJSON(Object obj) {
        if (obj instanceof Iterable)
            return mapper.convertValue(obj, JSONArray.class);

        return mapper.convertValue(obj, JSONObject.class);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public final void call(Object... args) {
        Ack cb = null;

        try {
            if (args == null || args.length == 0)
                throw new IllegalArgumentException("Missing arguments.");

            if (args.length > 2)
                throw new IllegalArgumentException("Wrong arguments count, must be <= 2: " + Arrays.toString(args));

            JSONObject lsnrArgs = null;

            if (args.length == 1) {
                if (args[0] instanceof JSONObject)
                    lsnrArgs = (JSONObject)args[0];
                else if (args[0] instanceof Ack)
                    cb = (Ack)args[0];
                else
                    throw new IllegalArgumentException("Wrong type of argument, must be JSONObject or Ack: " + args[0]);
            }
            else {
                if (args[0] != null && !(args[0] instanceof JSONObject))
                    throw new IllegalArgumentException("Wrong type of argument, must be JSONObject: " + args[0]);

                if (!(args[1] instanceof Ack))
                    throw new IllegalArgumentException("Wrong type of argument, must be Ack: " + args[1]);

                lsnrArgs = (JSONObject)args[0];

                cb = (Ack)args[1];
            }

            Object res = execute(lsnrArgs == null ? Collections.emptyMap() : mapper.convertValue(lsnrArgs, Map.class));

            if (cb != null)
                cb.call(null, toJSON(res));
        }
        catch (Exception e) {
            if (cb != null)
                cb.call(e, null);
        }
    }

    /**
     * @param args map with method args.
     */
    public abstract Object execute(Map<String, Object> args) throws Exception;
}
