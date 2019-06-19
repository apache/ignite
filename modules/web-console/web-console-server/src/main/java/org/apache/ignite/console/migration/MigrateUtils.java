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

package org.apache.ignite.console.migration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.F;
import org.bson.Document;
import org.bson.types.ObjectId;

import static java.util.stream.Collectors.toList;

/**
 * Utility functions for migration.
 */
public class MigrateUtils {
    /**
     * Private constructor for utility class.
     */
    private MigrateUtils() {
        // No-op.
    }

    /**
     * @param doc Mongo document.
     * @param key Key.
     * @return Array of primitives.
     */
    public static int[] asPrimitives(Document doc, String key) {
        List<Integer> list = doc.getList(key, Integer.class);

        if (list == null)
            list = Collections.emptyList();

        return list.stream().mapToInt(i -> i).toArray();
    }

    /**
     * @param doc Mongo document.
     * @param key Key.
     * @return List of long values.
     */
    public static List<Long> asListOfLongs(Document doc, String key) {
        List<Long> res = new ArrayList<>();

        List<Object> rawData = doc.getList(key, Object.class);

        if (!F.isEmpty(rawData))
            rawData.forEach(item -> res.add(((Number)item).longValue()));

        return res;
    }

    /**
     * @param doc Mongo document.
     * @param key Key.
     * @return List of ObjectIDs values.
     */
    public static List<ObjectId> asListOfObjectIds(Document doc, String key) {
        List<ObjectId> res = new ArrayList<>();

        List<Object> rawData = (List<Object>)doc.get(key);

        if (!F.isEmpty(rawData))
            rawData.forEach(item -> {
                if (item != null)
                    res.add((ObjectId)item);
            });

        return res;
    }

    /**
     * @param ids Collection of UUIDs.
     * @return Collection of string values.
     */
    public static List<String> asStrings(Collection<UUID> ids) {
        return ids.stream().map(UUID::toString).collect(toList());
    }

    /**
     * @param doc Mongo document to cleanup.
     */
    @SuppressWarnings("unchecked")
    public static void mongoCleanup(Document doc) {
        doc.remove("_id");
        doc.remove("__v");
        doc.remove("space");

        Set<String> keys = doc.keySet();

        keys.forEach(key -> {
            Object val = doc.get(key);

            if (val instanceof Document)
                mongoCleanup((Document)val);

            if (val instanceof List) {
                List valAsList = (List)val;

                if (!F.isEmpty(valAsList) && valAsList.get(0) instanceof Document)
                    valAsList.forEach(item -> mongoCleanup((Document)item));
            }
        });
    }

    /**
     * @param doc Mongo document.
     * @return JSON for specified document.
     */
    public static String mongoToJson(Document doc) {
        mongoCleanup(doc);

        return doc.toJson();
    }

    /**
     * @param parent Parent Mongo document.
     * @param child Child document key.
     * @return Child Mongo document.
     */
    public static Document getDocument(Document parent, String child) {
        return (Document)parent.get(child);
    }

    /**
     * @param doc Mongo document.
     * @param key Key with string value.
     * @param dflt Default value if the value is {@code null}.
     * @return String value.
     */
    public static String getString(Document doc, String key, String dflt) {
        Object val = doc.get(key);

        if (val == null)
            return dflt;

        return val.toString();
    }

    /**
     * @param doc Mongo document.
     * @param key Key with integer value.
     * @param dflt Default value if the value is {@code null}.
     * @return Integer value.
     */
    public static int getInteger(Document doc, String key, int dflt) {
        Object val = doc.get(key);

        if (val == null)
            return dflt;

        if (val instanceof Number)
            return ((Number)val).intValue();

        if (val instanceof String)
            return Integer.parseInt(val.toString());

        throw new ClassCastException("Expected integer value, but found: " + val);
    }

    /**
     * @param doc Mongo document.
     * @param key Key with long value.
     * @return Long value.
     */
    public static long getLong(Document doc, String key) {
        Object val = doc.get(key);

        if (val instanceof Number)
            return ((Number)val).longValue();

        if (val instanceof String)
            return Long.parseLong(val.toString());

        throw new ClassCastException("Expected long value, but found: " + val);
    }

    /**
     * @param doc Mongo document.
     * @param key Key with long value.
     * @param dflt Default value if the value is {@code null}.
     * @return Long value.
     */
    public static long getLong(Document doc, String key, long dflt) {
        Object val = doc.get(key);

        if (val == null)
            return dflt;

        if (val instanceof Number)
            return ((Number)val).longValue();

        if (val instanceof String)
            return Long.parseLong(val.toString());

        throw new ClassCastException("Expected long value, but found: " + val);
    }

    /**
     * Map Mongo IDs to new IDs.
     *
     * @param mongoIds Collection with Mongo IDs.
     * @param mapping Mappings.
     * @return Collection of new IDs.
     */
    public static List<String> mongoIdsToNewIds(List<ObjectId> mongoIds, Map<ObjectId, UUID> mapping) {
        return mongoIds
            .stream()
            .map(mapping::get)
            .filter(Objects::nonNull)
            .map(UUID::toString)
            .collect(toList());

    }
}
