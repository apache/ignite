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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Reflection;
    using Apache.Ignite.Core.Impl.Common;

    /**
     * <summary>Collection info helper.</summary>
     */
    internal class PortableCollectionInfo
    {
        /** Cache "none" value. */
        private static readonly PortableCollectionInfo None =
            new PortableCollectionInfo(null, null, null);

        /** Cached infos. */
        private static readonly ConcurrentDictionary<Type, PortableCollectionInfo> Infos =
            new ConcurrentDictionary<Type, PortableCollectionInfo>(64, 32);

        /**
         * <summary>Get collection info for type.</summary>
         * <param name="type">Type.</param>
         * <returns>Collection info.</returns>
         */
        public static PortableCollectionInfo GetInstance(Type type)
        {
            return Infos.GetOrAdd(type, CreateInstance);
        }

        /**
         * <summary>Internal routine to get collection info for type.</summary>
         * <param name="type">Type.</param>
         * <returns>Collection info.</returns>
         */
        private static PortableCollectionInfo CreateInstance(Type type)
        {
            if (type.IsArray)
                return GetGenericArrayInfo(type);

            if (type.IsGenericType)
            {
                if (type.GetGenericTypeDefinition() == PortableUtils.TypGenericDictionary)
                    return GetGenericDictionaryInfo(type, type);

                var genTyp = type.GetInterface(PortableUtils.TypGenericDictionary.FullName);

                if (genTyp != null)
                    return GetGenericDictionaryInfo(type, genTyp);

                if (type.GetGenericTypeDefinition() == PortableUtils.TypGenericCollection)
                    return GetGenericCollectionInfo(type, type);

                genTyp = type.GetInterface(PortableUtils.TypGenericCollection.FullName);

                if (genTyp != null)
                    return GetGenericCollectionInfo(type, genTyp);
            }

            return None;
        }

        /// <summary>
        /// Gets the generic collection information.
        /// </summary>
        /// <param name="type">Original type.</param>
        /// <param name="genType">Generic collection type.</param>
        private static PortableCollectionInfo GetGenericCollectionInfo(Type type, Type genType)
        {
            var typeArguments = genType.GetGenericArguments();

            var writeMthd = PortableUtils.MtdhWriteGenericCollection.MakeGenericMethod(typeArguments);
            var readMthd = PortableUtils.MtdhReadGenericCollection.MakeGenericMethod(typeArguments);
            var ctorInfo = type.GetConstructor(new[] { typeof(int) });

            return new PortableCollectionInfo(writeMthd, readMthd, ctorInfo);
        }

        /// <summary>
        /// Gets the generic collection information.
        /// </summary>
        /// <param name="type">Original type.</param>
        private static PortableCollectionInfo GetGenericArrayInfo(Type type)
        {
            var typeArguments = type.GetElementType();

            var writeMthd = PortableUtils.MtdhWriteGenericCollection.MakeGenericMethod(typeArguments);
            var readMthd = PortableUtils.MtdhReadGenericArray.MakeGenericMethod(typeArguments);
            var ctorInfo = type.GetConstructor(new[] { typeof(int) });

            return new PortableCollectionInfo(writeMthd, readMthd, ctorInfo);
        }

        /// <summary>
        /// Gets the generic dictionary information.
        /// </summary>
        /// <param name="type">Original type.</param>
        /// <param name="genType">Generic collection type.</param>
        private static PortableCollectionInfo GetGenericDictionaryInfo(Type type, Type genType)
        {
            var typeArguments = genType.GetGenericArguments();

            var writeMthd = PortableUtils.MtdhWriteGenericDictionary.MakeGenericMethod(typeArguments);
            var readMthd = PortableUtils.MtdhReadGenericDictionary.MakeGenericMethod(typeArguments);
            var ctorInfo = type.GetConstructor(new[] { typeof(int) });

            return new PortableCollectionInfo(writeMthd, readMthd, ctorInfo);
        }

        /** Generic write func. */
        private readonly Action<object, PortableWriterImpl> _writeFunc;

        /** Generic read func. */
        private readonly Func<PortableReaderImpl, PortableCollectionInfo, object> _readFunc;
        
        /** Constructor func. */
        private readonly Func<object, object> _ctor;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableCollectionInfo"/> class.
        /// </summary>
        /// <param name="writeMethod">The write method.</param>
        /// <param name="readMethod">The read method.</param>
        /// <param name="ctorInfo">The ctor information.</param>
        private PortableCollectionInfo(MethodInfo writeMethod, MethodInfo readMethod, ConstructorInfo ctorInfo)
        {
            if (writeMethod != null)
                _writeFunc = DelegateConverter.CompileFunc<Action<object, PortableWriterImpl>>(null, writeMethod, null,
                    new[] {true, false, false});

            if (readMethod != null)
                _readFunc = DelegateConverter.CompileFunc<Func<PortableReaderImpl, PortableCollectionInfo, object>>(null, readMethod, 
                    null, new[] {false, false, false});

            if (ctorInfo != null)
                _ctor = DelegateConverter.CompileCtor<Func<object, object>>(ctorInfo, new[] {typeof (int)});
        }

        /// <summary>
        /// Gets a value indicating whether this instance represents any generic collection.
        /// </summary>
        public bool IsAny
        {
            get { return _readFunc != null || _writeFunc != null; }
        }

        /// <summary>
        /// Gets the constructor func.
        /// </summary>
        public Func<object, object> Constructor
        {
            get { return _ctor; }
        }

        /// <summary>
        /// Reads the generic collection.
        /// </summary>
        public object ReadGeneric(PortableReaderImpl reader)
        {
            Debug.Assert(reader != null);
            Debug.Assert(_readFunc != null);

            return _readFunc(reader, this);
        }

        /// <summary>
        /// Writes the generic collection.
        /// </summary>
        public void WriteGeneric(PortableWriterImpl writer, object value)
        {
            Debug.Assert(writer != null);
            Debug.Assert(_writeFunc != null);

            _writeFunc(value, writer);
        }
    }
}
