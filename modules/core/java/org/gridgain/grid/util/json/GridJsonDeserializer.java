// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.json;

import net.sf.json.*;
import org.apache.commons.beanutils.*;
import org.apache.commons.beanutils.converters.*;
import org.apache.commons.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * This class deserializes JSON string to Java object.
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings( {"UnnecessaryFullyQualifiedName", "SizeReplaceableByIsEmpty", "IfMayBeConditional"})
public class GridJsonDeserializer {
    /** Class name descriptor's key. */
    private static final String AT_CLASS = "@class";

    /** Collection element class name descriptor key. */
    private static final String AT_ELEM = "@elem";

    /**
     * Initialises date converters and register them.
     */
    static {
        String[] patterns = { "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss" };

        DateTimeConverter c1 = new DateConverter(null);
        DateTimeConverter c2 = new CalendarConverter(null);

        c1.setPatterns(patterns);
        c2.setPatterns(patterns);

        ConvertUtils.register(c1, java.util.Date.class);
        ConvertUtils.register(c2, java.util.Calendar.class);
    }

    /**
     * Deserializes JSON string and inject data to passed in object.
     *
     * @param obj Object for injecting.
     * @param json JSON string for deserialization.
     * @return Passed object with injected JSON content.
     * @throws GridException If any error occurs while deserialization or injecting.
     */
    public static Object inject(Object obj, String json) throws GridException {
        try {
            return injectJson(obj, json);
        }
        catch (JSONException e) {
            throw new GridException("Failed to deserialize or inject JSON properties: " + json, e);
        }
    }

    /**
     * Deserializes JSON string into object.
     *
     * @param json JSON string for deserialization.
     * @return Object representing deserialized JSON properties.
     * @throws GridException If any error occurs while deserialization or injecting.
     */
    public static Object deserialize(String json) throws GridException {
        try {
            return deserialize(JSONSerializer.toJSON(json), null);
        }
        catch (JSONException e) {
            throw new GridException("Failed to deserialize JSON properties: " + json, e);
        }
    }

    /**
     *  Ensures singleton.
     */
    private GridJsonDeserializer() {

    }

    /**
     * Deserializes JSON string and inject data to passed object.
     *
     * @param obj Passed object for injecting.
     * @param json JSON string for deserialization.
     * @return Passed object with injected JSON content.
     * @throws GridException If any error occurs while deserialization or injecting.
     */
    private static Object injectJson(Object obj, String json) throws GridException {
        assert obj != null;
        assert json != null;

        Object desObj = deserialize(json);

        if (desObj != null)
            if (Map.class.isAssignableFrom(desObj.getClass()))
                mapInject(obj, (Map)desObj);
            else
                objectInject(obj, desObj);

        return obj;
    }

    /**
     * Injects data from map to object.
     *
     * @param obj Object for injecting.
     * @param map Map with data.
     * @throws GridException If any exception occurs while data injecting.
     */
    @SuppressWarnings("unchecked")
    private static void mapInject(Object obj, Map map) throws GridException {
        assert obj != null;
        assert map != null;

        if (map.size() > 0) {
            Method[] mtds = obj.getClass().getMethods();

            for (String key : (Set<String>)map.keySet()) {
                boolean isFound = false;

                String mtdName = "set" + StringUtils.capitalize(key);

                for (Method mtd : mtds)
                    if (mtd.getName().equals(mtdName) && mtd.getParameterTypes().length == 1 &&
                        mtd.getAnnotation(GridSpiConfiguration.class) != null) {
                        Object val = map.get(key);

                        Class cls = mtd.getParameterTypes()[0];

                        if (val == null || !cls.isAssignableFrom(val.getClass()))
                            val = ConvertUtils.convert(val, cls);

                        try {
                            mtd.invoke(obj, val);
                        }
                        catch (IllegalAccessException | InvocationTargetException e) {
                            throw new GridException("Can't set value " + val + " with method " + mtdName + ".", e);
                        }

                        isFound = true;

                        break;
                    }

                if (!isFound)
                    throw new GridException("Method " + mtdName + " not found or not annotated with @" +
                        GridSpiConfiguration.class.getSimpleName() + " in object " + obj + '.');
            }
        }
    }

    /**
     * Injects data from one object to another.
     *
     * @param toObj Object for injecting.
     * @param fromObj Object with data.
     * @throws GridException Thrown if any error occurs while data injecting.
     */
    private static void objectInject(Object toObj, Object fromObj) throws GridException {
        assert toObj != null;
        assert fromObj != null;

        for (Method fromMtd : fromObj.getClass().getMethods()) {
            String mtdName = fromMtd.getName();

            if (mtdName.startsWith("get") && fromMtd.getParameterTypes().length == 0 &&
                fromMtd.getReturnType() != Void.TYPE && !"getClass".equals(mtdName)) {

                Object val;

                try {
                    val = fromMtd.invoke(fromObj);
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new GridException("Can't get value with method " + mtdName + ".", e);
                }

                mtdName = "set" + mtdName.substring(3, mtdName.length());

                Method mtd;

                try {
                    mtd = toObj.getClass().getMethod(mtdName, fromMtd.getReturnType());
                }
                catch (NoSuchMethodException ignore) {
                    throw new GridException("Method " + mtdName + " not found.");
                }

                if (mtd.getAnnotation(GridSpiConfiguration.class) == null)
                    throw new GridException("Method " + mtdName + " not found or not annotated with @" +
                        GridSpiConfiguration.class.getSimpleName() + " in object " + toObj + '.');

                try {
                    mtd.invoke(toObj, val);
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new GridException("Can't set value " + val + " with method " + mtdName + ".", e);
                }
            }
        }
    }

    /**
     * Deserializes passed object to Java object with given type.
     *
     * @param obj Object for deserialization.
     * @param type {@code Type} of resulted object.
     * @return Deserialized object.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    @Nullable private static Object deserialize(Object obj, @Nullable Type type) throws GridException {
        assert obj != null;

        Object res;

        if (obj instanceof JSONObject)
            res = deserializeObject((JSONObject)obj, type);
        else if (obj instanceof JSONArray)
            res = deserializeArray((JSONArray)obj, type);
        else if (obj instanceof JSONNull)
            res = null;
        else
            res = processObject(obj, type);

        return res;
    }

    /**
     * Deserializes passed {@link JSONObject} to object (or map) with default type (if specified).
     *
     * @param obj Passed {@code JSONObject} for deserialization.
     * @param type {@code Type} of the resulted object (if specified).
     * @return Deserialized object (or map).
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    private static Object deserializeObject(JSONObject obj, @Nullable Type type) throws GridException {
        assert obj != null;

        Class cls = getObjectClass(obj, retrieveClass(type));

        return Map.class.isAssignableFrom(cls) ? buildMap(obj, cls, type) : buildObject(obj, cls);
    }

    /**
     * Deserializes passed {@link JSONArray} to object (collection or array) with default type (if specified).
     *
     * @param arr Passed {@code JSONArray} for deserialization.
     * @param type {@code Type} of the resulted object (if specified).
     * @return Deserialized object (collection or array).
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    private static Object deserializeArray(JSONArray arr, @Nullable Type type) throws GridException {
        assert arr != null;

        Class cls = getArrayClass(arr, retrieveClass(type));

        Object res;

        if (cls.isArray())
            res = buildArray(arr, cls);
        else if (Iterable.class.isAssignableFrom(cls))
            res = buildCollection(arr, cls, type);
        else
            throw new GridException("Unknown collection class name: " + cls.getName() + " for JSON array: " + arr);

        return res;
    }

    /**
     * Deserializes {@link JSONArray} to Java array of given class.
     *
     * @param arr Array for deserialization.
     * @param cls Class of resulted array.
     * @return Deserialized array.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    @SuppressWarnings("unchecked")
    private static Object buildArray(JSONArray arr, Class cls) throws GridException {
        assert arr != null;
        assert cls != null;

        cls = cls.getComponentType();

        List list = new ArrayList(arr.size());

        for (Object elem : arr)
            if (!isDescriptor(elem))
                list.add(elem);

        int size = list.size();

        Object res = Array.newInstance(cls, size);

        for (int i = 0; i < size; i++)
            Array.set(res, i, deserialize(list.get(i), cls));

        return res;
    }

    /**
     * Deserializes {@link JSONArray} to Java collection of given class.
     *
     * @param arr Array for deserialization.
     * @param cls Class of resulted collection.
     * @param type Initial class of collection with type arguments.
     * @return Deserialized collection.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    @SuppressWarnings("unchecked")
    private static Object buildCollection(JSONArray arr, Class cls, @Nullable Type type) throws GridException {
        assert arr != null;
        assert cls != null;

        Class elemCls = getArrayElementClass(arr, retrieveTypeArgumentClass(type, 0));

        Collection res = (Collection)newInstance(cls);

        for (Object elem : arr)
            if (!isDescriptor(elem))
                res.add(deserialize(elem, elemCls != null ? elemCls : getDefaultClass(elem)));

        return res;
    }

    /**
     * Deserializes {@link JSONObject} to Java map of given class.
     *
     * @param obj Object for deserialization.
     * @param cls Class of resulted map.
     * @param type Initial class of map with type arguments.
     * @return Deserialized map.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    @SuppressWarnings("unchecked")
    private static Object buildMap(JSONObject obj, Class cls, @Nullable Type type) throws GridException {
        assert obj != null;
        assert cls != null;

        Class elemCls = getObjectElementClass(obj, retrieveTypeArgumentClass(type, 1));

        Map res = (Map)newInstance(cls);

        for (String key : (Set<String>)obj.keySet())
            if (!isDescriptor(key)) {
                Object elem = obj.get(key);

                res.put(key, deserialize(elem, elemCls != null ? elemCls : getDefaultClass(elem)));
            }

        return res;
    }

    /**
     * Deserializes {@link JSONObject} to Java object of given class.
     *
     * @param obj Object for deserialization.
     * @param cls Class of resulted object.
     * @return Deserialized object.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    @SuppressWarnings("unchecked")
    private static Object buildObject(JSONObject obj, Class cls) throws GridException {
        assert obj != null;
        assert cls != null;

        Object res = newInstance(cls);

        Method[] mtds = cls.getMethods();

        for (String key : (Set<String>)obj.keySet())
            if (!isDescriptor(key)) {
                String mtdName = "set" + StringUtils.capitalize(key);

                for (Method mtd : mtds)
                    if (mtd.getName().equals(mtdName) && mtd.getParameterTypes().length == 1) {
                        Object param = deserialize(obj.get(key), mtd.getGenericParameterTypes()[0]);

                        try {
                            mtd.invoke(res, param);
                        }
                        catch (IllegalAccessException | InvocationTargetException e) {
                            throw new GridException("Can't set value " + param + " with method " + mtdName + " of class " + cls +
                                ".", e);
                        }

                        break;
                    }
            }

        return res;
    }

    /**
     * Processes object while deserialization. If required, converts to the passed type.
     *
     * @param obj Object being processed.
     * @param type {@code Type} for convert.
     * @return Processed object.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    private static Object processObject(Object obj, @Nullable Type type) throws GridException {
        assert obj != null;

        Object res = obj;

        Class cls = retrieveClass(type);

        if (cls != null && !cls.isAssignableFrom(obj.getClass()))
            res = ConvertUtils.convert(obj.toString(), cls);

        return res;
    }

    /**
     * Retrieves {@link JSONObject} class for deserialization.
     *
     * @param obj Object for which to get object class.
     * @param dfltCls Default class.
     * @return Class of the passed array.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    private static Class getObjectClass(JSONObject obj, @Nullable Class dfltCls) throws GridException {
        assert obj != null;

        Class cls = null;

        if (obj.containsKey(AT_CLASS))
            cls = loadClass(obj, AT_CLASS);

        if (cls == null)
            if (dfltCls != null)
                cls = dfltCls;
            else
                cls = getDefaultClass(obj);

        assert cls != null;

        if (cls.isInterface() && Map.class.isAssignableFrom(cls))
            cls = HashMap.class;

        return cls;
    }

    /**
     * Retrieves {@link JSONObject} element class for deserialization.
     *
     * @param obj Object from which retrieve element class.
     * @param dfltCls Default element class.
     * @return Element class.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    @Nullable private static Class getObjectElementClass(JSONObject obj, @Nullable Class dfltCls) throws GridException {
        assert obj != null;

        Class cls = null;

        if (obj.containsKey(AT_ELEM))
            cls = loadClass(obj, AT_ELEM);

        if (cls == null && dfltCls != null)
            cls = dfltCls;

        return cls;
    }

    /**
     * Retrieves {@link JSONArray} class for deserialization.
     *
     * @param arr Array for which retrieve class.
     * @param dfltCls Default class.
     * @return Class of the passed array.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    private static Class getArrayClass(JSONArray arr, @Nullable Class dfltCls) throws GridException {
        assert arr != null;

        Class cls = null;

        if (arr.size() > 0) {
            boolean isFound = false;

            for (Object elem : arr)
                if (elem instanceof JSONObject) {
                    JSONObject obj = (JSONObject)elem;

                    if (obj.containsKey(AT_CLASS))
                        if (!isFound) {
                            cls = loadClass(obj, AT_CLASS);

                            isFound = true;
                        }
                        else
                            throw new GridException("JSON array can contain only one element with attribute " +
                                AT_CLASS + ": " + arr);
                }
        }

        if (cls == null)
            if (dfltCls != null)
                cls = dfltCls;
            else
                cls = getDefaultClass(arr);

        assert cls != null;

        if ((cls.isInterface() || Modifier.isAbstract(cls.getModifiers())) && Iterable.class.isAssignableFrom(cls))
            if (Set.class.isAssignableFrom(cls))
                cls = HashSet.class;
            else if (Queue.class.isAssignableFrom(cls))
                cls = LinkedList.class;
            else
                cls = ArrayList.class;

        return cls;
    }

    /**
     * Retrieves {@link JSONArray} element class for deserialization.
     *
     * @param arr Array from which retrieve element class.
     * @param dfltCls Default element class.
     * @return Element class.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    @Nullable private static Class getArrayElementClass(JSONArray arr, @Nullable Class dfltCls) throws GridException {
        assert arr != null;

        Class cls = null;

        if (arr.size() > 0) {
            boolean isFound = false;

            for (Object elem : arr)
                if (elem instanceof JSONObject) {
                    JSONObject obj = (JSONObject)elem;

                    if (obj.containsKey(AT_ELEM))
                        if (!isFound) {
                            cls = loadClass(obj, AT_ELEM);

                            isFound = true;
                        }
                        else
                            throw new GridException("JSON array can contain only one element with attribute " +
                                AT_ELEM + ": " + arr);
                }
        }

        if (cls == null && dfltCls != null)
            cls = dfltCls;

        return cls;
    }

    /**
     * Loads class by name. Name is a value of the passed {@link JSONObject}
     * attribute with the specified {@code key}.
     *
     * @param obj Object which contains class name.
     * @param key Attribute key.
     * @return Loaded class.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    private static Class loadClass(JSONObject obj, String key) throws GridException {
        assert obj != null;
        assert key != null;

        String clsName = obj.getString(key);

        try {
            return Class.forName(clsName);
        }
        catch (ClassNotFoundException e) {
            throw new GridException("Can't load class " + clsName + " for JSON object: " + obj, e);
        }
    }

    /**
     * Creates new instance of the passed in class.
     *
     * @param cls Class which instance should create.
     * @return New instance.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    private static Object newInstance(Class cls) throws GridException {
        assert cls != null;

        try {
            return cls.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new GridException("Can't create instance of " + cls + " for JSON object.", e);
        }
    }

    /**
     * Retrieves class from the passed type.
     *
     * @param type Type from which should retrieve class.
     * @return Retrieved class.
     * @throws GridException Thrown if any error occurs while deserialization.
     */
    @Nullable private static Class retrieveClass(@Nullable Type type) throws GridException {
        Class cls;

        if (type == null)
            cls = null;
        else if (type instanceof Class)
            cls = (Class)type;
        else if (type instanceof ParameterizedType)
            cls = (Class)((ParameterizedType)type).getRawType();
        else
            throw new GridException("Can't determine class from type: " + type);

        return cls;
    }

    /**
     * Gets array with type arguments of the passed type.
     *
     * @param type Type from which retrieve type arguments.
     * @return Array with type arguments if the passed type have them or {@code null} - otherwise.
     */
    @Nullable private static Type[] getTypeArguments(@Nullable Type type) {
        Type[] types = null;

        if (type != null && type instanceof ParameterizedType)
            types = ((ParameterizedType)type).getActualTypeArguments();

        return types;
    }

    /**
     * If the passed type contains type arguments retrieves class for one of them.
     *
     * @param type Type from which should retrieve type argument's class.
     * @param idx Type argument's index.
     * @return Class if the passed type contains type argument with the specified index -
     *      {@code null} otherwise.
     * @throws GridException If it's impossible to determine type argument's class.
     */
    @Nullable private static Class retrieveTypeArgumentClass(@Nullable Type type, int idx) throws GridException {
        assert idx >= 0;

        Class cls = null;

        Type[] types = getTypeArguments(type);

        if (types != null && types.length > idx)
            cls = retrieveClass(types[idx]);

        return cls;
    }

    /**
     * Gets default class for JSON object deserialization.
     *
     * @param obj Object for default class determination.
     * @return Default class for object.
     */
    @Nullable private static Class getDefaultClass(@Nullable Object obj) {
        Class cls = null;

        if (obj != null)
            if (obj instanceof JSONObject)
                cls = HashMap.class;
            else if (obj instanceof JSONArray)
                cls = ArrayList.class;

        return cls;
    }

    /**
     * Checks if the passed key is a descriptor's key.
     *
     * @param key Checked key.
     * @return {@code true} if passed key is a descriptor's key, {@code false} - otherwise.
     */
    private static boolean isDescriptor(@Nullable String key) {
        return AT_CLASS.equals(key) || AT_ELEM.equals(key);
    }

    /**
     * Checks if the passed object is a descriptor.
     *
     * @param obj Object to check.
     * @return {@code true} if passed object is a descriptor, {@code false} - otherwise.
     */
    private static boolean isDescriptor(@Nullable Object obj) {
        if (obj != null && obj instanceof JSONObject) {
            Map json = (Map)obj;

            return json.containsKey(AT_CLASS) || json.containsKey(AT_ELEM);
        }

        return false;
    }
}
