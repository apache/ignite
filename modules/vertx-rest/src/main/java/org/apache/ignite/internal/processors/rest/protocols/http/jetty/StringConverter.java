package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.jackson.IgniteBinaryObjectJsonDeserializer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StringConverter {

	 /** Cache name. */
    final String cacheName;
    
    final ObjectMapper jsonMapper;

    /**
     * @param cacheName Cache name.
     */
    public StringConverter(String cacheName,ObjectMapper jsonMapper) {
        this.cacheName = cacheName;
        this.jsonMapper = jsonMapper;
    }

    /** */
    public StringConverter(ObjectMapper jsonMapper) {
        this(null,jsonMapper);
    }

    /**
     * Gets and converts values referenced by sequential keys, e.g. {@code key1...keyN}.
     *
     * @param type Optional value type.
     * @param keyPrefix Key prefix, e.g. {@code key} for {@code key1...keyN}.
     * @param params Parameters map.
     * @return Values.
     * @throws IgniteCheckedException If failed to convert.
     */
    public List<Object> values(String type, String keyPrefix, Map<String, String> params) throws IgniteCheckedException {
        assert keyPrefix != null;

        List<Object> vals = new LinkedList<>();

        for (int i = 1; ; i++) {
            String key = keyPrefix + i;

            if (params.containsKey(key))
                vals.add(convert(type, params.get(key)));
            else
                break;
        }

        return vals;
    }

    /**
     * @param type Optional value type.
     * @param str String to convert.
     * @return Converted value.
     * @throws IgniteCheckedException If failed to convert.
     */
    public Object convert(@Nullable String type, @Nullable String str) throws IgniteCheckedException {
        if (F.isEmpty(type) || str == null)
            return str;

        try {
            switch (type.toLowerCase()) {
                case "boolean":
                case "java.lang.boolean":
                    return Boolean.valueOf(str);

                case "byte":
                case "java.lang.byte":
                    return Byte.valueOf(str);

                case "short":
                case "java.lang.short":
                    return Short.valueOf(str);

                case "int":
                case "integer":
                case "java.lang.integer":
                    return Integer.valueOf(str);

                case "long":
                case "java.lang.long":
                    return Long.valueOf(str);

                case "float":
                case "java.lang.float":
                    return Float.valueOf(str);

                case "double":
                case "java.lang.double":
                    return Double.valueOf(str);

                case "date":
                case "java.sql.date":
                    return Date.valueOf(str);

                case "time":
                case "java.sql.time":
                    return Time.valueOf(str);

                case "timestamp":
                case "java.sql.timestamp":
                    return Timestamp.valueOf(str);

                case "uuid":
                case "java.util.uuid":
                    return UUID.fromString(str);

                case "igniteuuid":
                case "org.apache.ignite.lang.igniteuuid":
                    return IgniteUuid.fromString(str);

                case "string":
                case "java.lang.string":
                    return str;
            }

            // Creating an object of the specified type, if its class is available.
            Class<?> cls = U.classForName(type, null);

            if (cls != null)
                return jsonMapper.readValue(str, cls);

            // Creating a binary object if the type is not a class name or it cannot be loaded.
            InjectableValues.Std prop = new InjectableValues.Std()
                .addValue(IgniteBinaryObjectJsonDeserializer.BINARY_TYPE_PROPERTY, type)
                .addValue(IgniteBinaryObjectJsonDeserializer.CACHE_NAME_PROPERTY, cacheName);

            return jsonMapper.reader(prop).forType(BinaryObject.class).readValue(str);
        }
        catch (Throwable e) {
            throw new IgniteCheckedException("Failed to convert value to specified type [type=" + type +
                ", val=" + str + ", reason=" + e.getClass().getName() + ": " + e.getMessage() + "]", e);
        }
    }
}
