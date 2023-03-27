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

namespace Apache.Ignite.Core.Binary
{
    using System;
    using System.Collections;

    /// <summary>
    /// binary object builder. Provides ability to build binary objects dynamically
    /// without having class definitions.
    /// <para />
    /// Note that type ID is required in order to build binary object. Usually it is
    /// enough to provide a simple type name and Ignite will generate the type ID
    /// automatically.
    /// </summary>
    public interface IBinaryObjectBuilder
    {
        /// <summary>
        /// Get object field value. If value is another binary object, then
        /// builder for this object will be returned. If value is a container
        /// for other objects (array, ICollection, IDictionary), then container
        /// will be returned with primitive types in deserialized form and
        /// binary objects as builders. Any change in builder or collection
        /// returned through this method will be reflected in the resulting
        /// binary object after build.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Field value.</returns>
        T GetField<T>(string fieldName);

        /// <summary>
        /// Set object field value. Value can be of any type including other
        /// <see cref="IBinaryObject"/> and other builders.
        /// <para />
        /// Value type for metadata is determined as <c>typeof(T)</c>;
        /// use <see cref="SetField{T}(string,T,Type)"/> overload to override.  
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Field value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetField<T>(string fieldName, T val);

        /// <summary>
        /// Set object field value. Value can be of any type including other
        /// <see cref="IBinaryObject"/> and other builders.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Field value.</param>
        /// <param name="valType">Field value type for metadata
        /// (see also <see cref="IBinaryType.GetFieldTypeName"/>).</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetField<T>(string fieldName, T val, Type valType);

        /// <summary>
        /// Remove object field.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder RemoveField(string fieldName);

        /// <summary>
        /// Build the object.
        /// </summary>
        /// <returns>Resulting binary object.</returns>
        IBinaryObject Build();

        /// <summary>
        /// Sets the array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetArrayField<T>(string fieldName, T[] val);

        /// <summary>
        /// Sets the boolean field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetBooleanField(string fieldName, bool val);

        /// <summary>
        /// Sets the boolean array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetBooleanArrayField(string fieldName, bool[] val);

        /// <summary>
        /// Sets the byte field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetByteField(string fieldName, byte val);

        /// <summary>
        /// Sets the byte array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetByteArrayField(string fieldName, byte[] val);

        /// <summary>
        /// Sets the char field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetCharField(string fieldName, char val);

        /// <summary>
        /// Sets the char array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetCharArrayField(string fieldName, char[] val);

        /// <summary>
        /// Sets the collection field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetCollectionField(string fieldName, ICollection val);

        /// <summary>
        /// Sets the decimal field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetDecimalField(string fieldName, decimal? val);

        /// <summary>
        /// Sets the decimal array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetDecimalArrayField(string fieldName, decimal?[] val);

        /// <summary>
        /// Sets the dictionary field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetDictionaryField(string fieldName, IDictionary val);

        /// <summary>
        /// Sets the double field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetDoubleField(string fieldName, double val);

        /// <summary>
        /// Sets the double array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetDoubleArrayField(string fieldName, double[] val);

        /// <summary>
        /// Sets the enum field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetEnumField<T>(string fieldName, T val);

        /// <summary>
        /// Sets the enum array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetEnumArrayField<T>(string fieldName, T[] val);

        /// <summary>
        /// Sets the float field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetFloatField(string fieldName, float val);

        /// <summary>
        /// Sets the float array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetFloatArrayField(string fieldName, float[] val);

        /// <summary>
        /// Sets the guid field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetGuidField(string fieldName, Guid? val);

        /// <summary>
        /// Sets the guid array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetGuidArrayField(string fieldName, Guid?[] val);

        /// <summary>
        /// Sets the int field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetIntField(string fieldName, int val);

        /// <summary>
        /// Sets the int array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetIntArrayField(string fieldName, int[] val);

        /// <summary>
        /// Sets the long field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetLongField(string fieldName, long val);

        /// <summary>
        /// Sets the long array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetLongArrayField(string fieldName, long[] val);

        /// <summary>
        /// Sets the short field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetShortField(string fieldName, short val);

        /// <summary>
        /// Sets the short array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetShortArrayField(string fieldName, short[] val);

        /// <summary>
        /// Sets the string field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetStringField(string fieldName, string val);

        /// <summary>
        /// Sets the string array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetStringArrayField(string fieldName, string[] val);

        /// <summary>
        /// Sets the timestamp field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetTimestampField(string fieldName, DateTime? val);

        /// <summary>
        /// Sets the timestamp array field.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="val">The value.</param>
        /// <returns>Current builder instance.</returns>
        IBinaryObjectBuilder SetTimestampArrayField(string fieldName, DateTime?[] val);
    }
}
