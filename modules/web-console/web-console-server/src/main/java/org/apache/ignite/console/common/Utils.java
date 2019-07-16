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

package org.apache.ignite.console.common;

import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.springframework.security.web.servletapi.SecurityContextHolderAwareRequestWrapper;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.springframework.security.web.authentication.switchuser.SwitchUserFilter.ROLE_PREVIOUS_ADMINISTRATOR;

/**
 * Utilities.
 */
public class Utils {
    /** */
    private static final JsonObject EMPTY_OBJ = new JsonObject();

    /**
     * @param cause Error.
     * @return Error message or exception class name.
     */
    public static String errorMessage(Throwable cause) {
        String msg = cause.getMessage();

        return F.isEmpty(msg) ? cause.getClass().getName() : msg;
    }

    /**
     * @param a First set.
     * @param b Second set.
     * @return Elements exists in a and not in b.
     */
    public static TreeSet<UUID> diff(TreeSet<UUID> a, TreeSet<UUID> b) {
        return a.stream().filter(item -> !b.contains(item)).collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * @param json JSON object.
     * @param key Key with IDs.
     * @return Set of IDs.
     */
    public static TreeSet<UUID> idsFromJson(JsonObject json, String key) {
        TreeSet<UUID> res = new TreeSet<>();

        JsonArray ids = json.getJsonArray(key);

        if (ids != null)
            ids.forEach(item -> res.add(UUID.fromString(item.toString())));

        return res;
    }

    /**
     * @param json JSON to travers.
     * @param path Dot separated list of properties.
     * @return Tuple with unwind JSON and key to extract from it.
     */
    private static T2<JsonObject, String> xpath(JsonObject json, String path) {
        String[] keys = path.split("\\.");

        for (int i = 0; i < keys.length - 1; i++) {
            json = json.getJsonObject(keys[i]);

            if (json == null)
                json = EMPTY_OBJ;
        }

        String key = keys[keys.length - 1];

        return new T2<>(json, key);
    }

    /**
     * @param json JSON object.
     * @param path Dot separated list of properties.
     * @param def Default value.
     * @return the value or {@code def} if no entry present.
     */
    public static boolean boolParam(JsonObject json, String path, boolean def) {
        T2<JsonObject, String> t = xpath(json, path);

        return t.getKey().getBoolean(t.getValue(), def);
    }

    /**
     * @param data Collection of DTO objects.
     * @return JSON array.
     */
    public static JsonArray toJsonArray(Collection<? extends DataObject> data) {
        JsonArray res = new JsonArray();

        data.forEach(item -> res.add(fromJson(item.json())));

        return res;
    }

    /**
     * @return Current request origin.
     */
    public static String currentRequestOrigin() {
        return ServletUriComponentsBuilder
            .fromCurrentRequest()
            .replacePath(null)
            .build()
            .toString();
    }

    /**
     * Is switch user used.
     * @param req Request wrapper.
     * @return Switch user used flag.
     */
    public static boolean isBecomeUsed(SecurityContextHolderAwareRequestWrapper req) {
        return req.isUserInRole(ROLE_PREVIOUS_ADMINISTRATOR);
    }
}
