/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework;

import net.sf.json.*;
import org.gridgain.grid.*;

import java.util.*;

/**
 * Utility class for tests.
 */
public class GridHadoopTestUtils {
    /**
     * Converts json string to Map<String,String>.
     *
     * @param jsonStr String to convert.
     * @return Map.
     * @throws GridException If fails.
     */
    public static Map<String,String> jsonToMap(String jsonStr) throws GridException {
        Map<String,String> res = new HashMap<>();

        try {
            JSONObject jsonObj = JSONObject.fromObject(jsonStr);

            for (Object o : jsonObj.entrySet()) {
                Map.Entry e = (Map.Entry) o;

                res.put(e.getKey().toString(), e.getValue().toString());
            }

        }
        catch (JSONException e) {
            throw new GridException(e);
        }

        return res;
    }
}
