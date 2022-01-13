/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network.serialization;

/**
 * Accessor for a specific field.
 */
public interface FieldAccessor {
    /**
     * Returns the bound object field value of the given object.
     *
     * @param target target object
     * @return the bound field value of the given object
     */
    Object getObject(Object target);

    /**
     * Sets the bound object field value on the given object.
     *
     * @param target     target object
     * @param fieldValue value to set
     */
    void setObject(Object target, Object fieldValue);

    /**
     * Returns the bound byte field value of the given object.
     *
     * @param target target object
     * @return the bound byte field value of the given object
     */
    byte getByte(Object target);

    /**
     * Sets the bound byte field value on the given object.
     *
     * @param target     target object
     * @param fieldValue value to set
     */
    void setByte(Object target, byte fieldValue);

    /**
     * Returns the bound short field value of the given object.
     *
     * @param target target object
     * @return the bound byte field value of the given object
     */
    short getShort(Object target);

    /**
     * Sets the bound short field value on the given object.
     *
     * @param target     target object
     * @param fieldValue value to set
     */
    void setShort(Object target, short fieldValue);

    /**
     * Returns the bound int field value of the given object.
     *
     * @param target target object
     * @return the bound int field value of the given object
     */
    int getInt(Object target);

    /**
     * Sets the bound int field value on the given object.
     *
     * @param target     target object
     * @param fieldValue value to set
     */
    void setInt(Object target, int fieldValue);

    /**
     * Returns the bound long field value of the given object.
     *
     * @param target target object
     * @return the bound byte field value of the given object
     */
    long getLong(Object target);

    /**
     * Sets the bound long field value on the given object.
     *
     * @param target     target object
     * @param fieldValue value to set
     */
    void setLong(Object target, long fieldValue);

    /**
     * Returns the bound float field value of the given object.
     *
     * @param target target object
     * @return the bound byte field value of the given object
     */
    float getFloat(Object target);

    /**
     * Sets the bound float field value on the given object.
     *
     * @param target     target object
     * @param fieldValue value to set
     */
    void setFloat(Object target, float fieldValue);

    /**
     * Returns the bound double field value of the given object.
     *
     * @param target target object
     * @return the bound byte field value of the given object
     */
    double getDouble(Object target);

    /**
     * Sets the bound double field value on the given object.
     *
     * @param target     target object
     * @param fieldValue value to set
     */
    void setDouble(Object target, double fieldValue);

    /**
     * Returns the bound char field value of the given object.
     *
     * @param target target object
     * @return the bound byte field value of the given object
     */
    char getChar(Object target);

    /**
     * Sets the bound char field value on the given object.
     *
     * @param target     target object
     * @param fieldValue value to set
     */
    void setChar(Object target, char fieldValue);

    /**
     * Returns the bound boolean field value of the given object.
     *
     * @param target target object
     * @return the bound byte field value of the given object
     */
    boolean getBoolean(Object target);

    /**
     * Sets the bound boolean field value on the given object.
     *
     * @param target     target object
     * @param fieldValue value to set
     */
    void setBoolean(Object target, boolean fieldValue);
}
