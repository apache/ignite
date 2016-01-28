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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Runtime.Serialization;

    /// <summary>
    /// Allows serializing object graphs with non-serializable anonymous types.
    /// </summary>
    [Serializable]
    internal class SerializableWrapper<T> : ISerializable
    {
        /** Type field. */
        private const string ValType = "Type";

        /** Field prefix to avoid clash with other data. */
        private const string ValFld = "Field.";
        
        /** Method field */
        private const string ValMethod = "Method";

        /** Target field. */
        private const string ValTarget = "Target";

        /** Wrapped object */
        private readonly T _obj;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableWrapper{T}"/> class.
        /// </summary>
        /// <param name="obj">The object to wrap.</param>
        public SerializableWrapper(T obj)
        {
            Debug.Assert(obj != null);
            Debug.Assert(obj is Delegate || IsCompilerGenerated(obj.GetType()));

            _obj = obj;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableWrapper{T}"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="context">Streaming context.</param>
        public SerializableWrapper(SerializationInfo info, StreamingContext context)
        {
            var objType = (Type) info.GetValue(ValType, typeof (Type));

            if (typeof (Delegate).IsAssignableFrom(objType))
            {
                var method = (MethodInfo) GetValue(info, ValMethod);
                var target = GetValue(info, ValTarget);

                _obj = TypeCaster<T>.Cast(Delegate.CreateDelegate(objType, target, method));
            }
            else
            {
                _obj = (T) FormatterServices.GetUninitializedObject(objType);

                foreach (var fld in GetFields())
                {
                    var value = GetValue(info, ValFld + fld.Name);

                    fld.SetValue(_obj, value);
                }
            }
        }

        /// <summary>
        /// Gets the wrapped object.
        /// </summary>
        public T WrappedObject
        {
            get { return _obj; }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(ValType, _obj.GetType());

            var del = _obj as Delegate;
            if (del != null)
            {
                AddValue(info, ValMethod, del.Method);
                AddValue(info, ValTarget, del.Target);
            }
            else
            {
                foreach (var fld in GetFields())
                {
                    var value = fld.GetValue(_obj);

                    var name = ValFld + fld.Name;

                    AddValue(info, name, value);
                }
            }
        }

        /// <summary>
        /// Gets the value.
        /// </summary>
        private static object GetValue(SerializationInfo info, string name)
        {
            var value = info.GetValue(name, typeof(object));

            var holder = value as SerializableWrapper<object>;

            if (holder != null)
                value = holder.WrappedObject;

            return value;
        }

        /// <summary>
        /// Adds the value.
        /// </summary>
        private static void AddValue(SerializationInfo info, string name, object value)
        {
            if (value != null && IsCompilerGenerated(value.GetType()))
                value = new SerializableWrapper<object>(value);

            info.AddValue(name, value);
        }

        /// <summary>
        /// Gets the fields.
        /// </summary>
        private IEnumerable<FieldInfo> GetFields()
        {
            return _obj.GetType().GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        }

        /// <summary>
        /// Determines whether specified type is compiler-generated.
        /// </summary>
        public static bool IsCompilerGenerated(Type type)
        {
            return type.GetCustomAttributes(typeof(CompilerGeneratedAttribute), false).Any();
        }
    }
}