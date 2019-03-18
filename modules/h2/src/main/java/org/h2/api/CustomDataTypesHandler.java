/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import org.h2.store.DataHandler;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * Custom data type handler
 * Provides means to plug-in custom data types support
 *
 * Please keep in mind that this feature may not possibly
 * provide the same ABI stability level as other features
 * as it  exposes many of the H2 internals.  You may be
 * required to update your code occasionally due to internal
 * changes in H2 if you are going to use this feature
 */
public interface CustomDataTypesHandler {
    /**
     * Get custom data type given its name
     *
     * @param name data type name
     * @return custom data type
     */
    DataType getDataTypeByName(String name);

    /**
     * Get custom data type given its integer id
     *
     * @param type identifier of a data type
     * @return custom data type
     */
    DataType getDataTypeById(int type);

    /**
     * Get order for custom data type given its integer id
     *
     * @param type identifier of a data type
     * @return order associated with custom data type
     */
    int getDataTypeOrder(int type);

    /**
     * Convert the provided source value into value of given target data type
     * Shall implement conversions to and from custom data types.
     *
     * @param source source value
     * @param targetType identifier of target data type
     * @return converted value
     */
    Value convert(Value source, int targetType);

    /**
     * Get custom data type class name given its integer id
     *
     * @param type identifier of a data type
     * @return class name
     */
    String getDataTypeClassName(int type);

    /**
     * Get custom data type identifier given corresponding Java class
     * @param cls Java class object
     * @return type identifier
     */
    int getTypeIdFromClass(Class<?> cls);

    /**
     * Get {@link org.h2.value.Value} object
     * corresponding to given data type identifier and data.
     *
     * @param type custom data type identifier
     * @param data underlying data type value
     * @param dataHandler data handler object
     * @return Value object
     */
    Value getValue(int type, Object data, DataHandler dataHandler);

    /**
     * Converts {@link org.h2.value.Value} object
     * to the specified class.
     *
     * @param value the value to convert
     * @param cls the target class
     * @return result
     */
    Object getObject(Value value, Class<?> cls);

    /**
     * Checks if type supports add operation
     *
     * @param type custom data type identifier
     * @return True, if custom data type supports add operation
     */
    boolean supportsAdd(int type);

    /**
     * Get compatible type identifier that would not overflow
     * after many add operations.
     *
     * @param type identifier of a type
     * @return resulting type identifier
     */
    int getAddProofType(int type);
}
