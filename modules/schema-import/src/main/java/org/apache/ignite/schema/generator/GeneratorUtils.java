package org.apache.ignite.schema.generator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.schema.model.PojoField;

/**
 * Schema import generator utils.
 */
public class GeneratorUtils {
    /** Map of conformity between primitive type and Java class. */
    private static final Map<String, String> primitiveToObject = new HashMap<>();

    static {
        primitiveToObject.put("boolean", "java.lang.Boolean");
        primitiveToObject.put("byte", "java.lang.Byte");
        primitiveToObject.put("short", "java.lang.Short");
        primitiveToObject.put("int", "java.lang.Integer");
        primitiveToObject.put("long", "java.lang.Long");
        primitiveToObject.put("float", "java.lang.Float");
        primitiveToObject.put("double", "java.lang.Double");
    }

    /**
     * Convert primitive type to conformity Java class.
     *
     * @param type Primitive type.
     * @return Conformity Java class
     */
    public static String boxPrimitiveType(String type) {
        if (primitiveToObject.containsKey(type))
            return primitiveToObject.get(type);

        return type;
    }

    /**
     * Find field by name.
     *
     * @param fields Field descriptors.
     * @param name Field name to find.
     * @return Field descriptor or {@code null} if not found.
     */
    public static PojoField findFieldByName(Collection<PojoField> fields, String name) {
        for (PojoField field: fields)
            if (field.dbName().equals(name))
                return field;

        return null;
    }
}
