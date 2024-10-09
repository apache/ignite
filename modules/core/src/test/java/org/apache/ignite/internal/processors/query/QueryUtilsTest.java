package org.apache.ignite.internal.processors.query;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.property.QueryBinaryProperty;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class QueryUtilsTest {

    @Test
    public void testBuildBinaryProperty()
    {
        GridKernalContext ctx = null;
        String pathStr = "user.address.streetName";
        Class<?> resType = null;
        Map<String, String> aliases = new HashMap<>();
        boolean isKeyField = false;
        boolean notNull = false;
        Object dlftVal = "DEFAULT";
        int precision = 10;
        int scale = 5;

        QueryBinaryProperty queryBinaryProperty = QueryUtils.buildBinaryProperty(ctx, pathStr, resType, aliases, isKeyField, notNull, dlftVal, precision, scale);

        Assert.assertNotNull(queryBinaryProperty);
        assertEquals(dlftVal, queryBinaryProperty.defaultValue());
        assertEquals(precision, queryBinaryProperty.precision());
        assertEquals(scale, queryBinaryProperty.scale());
        assertEquals(pathStr, queryBinaryProperty.name());
        assertEquals(notNull, queryBinaryProperty.notNull());
        assertEquals(resType, queryBinaryProperty.type());
    }

    @Test
    public void testBuildBinaryPropertyWithAlias()
    {
        GridKernalContext ctx = null;
        String pathStr = "user.address.streetName";
        String alias = "user_address_streetName";
        Class<?> resType = null;
        Map<String, String> aliases = new HashMap<>();
        aliases.put(pathStr, alias);
        boolean isKeyField = false;
        boolean notNull = false;
        Object dlftVal = "DEFAULT";
        int precision = 10;
        int scale = 5;

        QueryBinaryProperty queryBinaryProperty = QueryUtils.buildBinaryProperty(ctx, pathStr, resType, aliases, isKeyField, notNull, dlftVal, precision, scale);

        Assert.assertNotNull(queryBinaryProperty);
        assertEquals(dlftVal, queryBinaryProperty.defaultValue());
        assertEquals(precision, queryBinaryProperty.precision());
        assertEquals(scale, queryBinaryProperty.scale());
        assertEquals(alias, queryBinaryProperty.name());
        assertEquals(notNull, queryBinaryProperty.notNull());
        assertEquals(resType, queryBinaryProperty.type());
    }
}