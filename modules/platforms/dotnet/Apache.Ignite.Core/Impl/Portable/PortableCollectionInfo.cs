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
        /** Flag: none. */
        private const byte FlagNone = 0;

        /** Flag: generic dictionary. */
        private const byte FlagGenericDictionary = 1;

        /** Flag: generic collection. */
        private const byte FlagGenericCollection = 2;

        /** Flag: dictionary. */
        private const byte FlagDictionary = 3;

        /** Flag: collection. */
        private const byte FlagCollection = 4;

        /** Cache "none" value. */
        private static readonly PortableCollectionInfo None =
            new PortableCollectionInfo(FlagNone, null, null, null, null);

        /** Cache "dictionary" value. */
        private static readonly PortableCollectionInfo Dictionary =
            new PortableCollectionInfo(FlagDictionary, PortableSystemHandlers.WriteHndDictionary,
                null, null, null);

        /** Cache "collection" value. */
        private static readonly PortableCollectionInfo Collection =
            new PortableCollectionInfo(FlagCollection, PortableSystemHandlers.WriteHndCollection,
                null, null, null);

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

            if (type == PortableUtils.TypDictionary || type.GetInterface(PortableUtils.TypDictionary.FullName) != null)
                return Dictionary;

            if (type == PortableUtils.TypCollection || type.GetInterface(PortableUtils.TypCollection.FullName) != null)
                return Collection;

            return None;
        }

        /// <summary>
        /// Gets the generic collection information.
        /// </summary>
        /// <param name="type">Original type.</param>
        /// <param name="genType">Generic collection type.</param>
        private static PortableCollectionInfo GetGenericCollectionInfo(Type type, Type genType)
        {
            var writeMthd = PortableUtils.MtdhWriteGenericCollection.MakeGenericMethod(genType.GetGenericArguments());
            var readMthd = PortableUtils.MtdhReadGenericCollection0.MakeGenericMethod(genType.GetGenericArguments());
            var ctorInfo = type.GetConstructor(new[] { typeof(int) });

            return new PortableCollectionInfo(FlagGenericCollection,
                PortableSystemHandlers.WriteHndGenericCollection, writeMthd, readMthd, ctorInfo);
        }

        /// <summary>
        /// Gets the generic dictionary information.
        /// </summary>
        /// <param name="type">Original type.</param>
        /// <param name="genType">Generic collection type.</param>
        private static PortableCollectionInfo GetGenericDictionaryInfo(Type type, Type genType)
        {
            var writeMthd = PortableUtils.MtdhWriteGenericDictionary.MakeGenericMethod(genType.GetGenericArguments());
            var readMthd = PortableUtils.MtdhReadGenericDictionary0.MakeGenericMethod(genType.GetGenericArguments());
            var ctorInfo = type.GetConstructor(new[] { typeof(int) });

            return new PortableCollectionInfo(FlagGenericDictionary,
                PortableSystemHandlers.WriteHndGenericDictionary, writeMthd, readMthd, ctorInfo);
        }

        /** Flag. */
        private readonly byte _flag;

        /** Write handler. */
        private readonly PortableSystemWriteDelegate _writeHnd;

        /** Generic write func. */
        private readonly Action<object, PortableWriterImpl> _writeFunc;

        /** Generic read func. */
        private readonly Func<PortableReaderImpl, object, PortableCollectionInfo, object> _readFunc;
        
        /** Constructor func. */
        private readonly Func<object, object> _ctor;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableCollectionInfo"/> class.
        /// </summary>
        /// <param name="flag">The flag.</param>
        /// <param name="writeHnd">The write handler.</param>
        /// <param name="writeMethod">The write method.</param>
        /// <param name="readMethod">The read method.</param>
        /// <param name="ctorInfo">The ctor information.</param>
        private PortableCollectionInfo(byte flag, PortableSystemWriteDelegate writeHnd,
            MethodInfo writeMethod, MethodInfo readMethod, ConstructorInfo ctorInfo)
        {
            _flag = flag;
            _writeHnd = writeHnd;

            if (writeMethod != null)
                _writeFunc = DelegateConverter.CompileFunc<Action<object, PortableWriterImpl>>(null, writeMethod, null,
                    new[] {true, false, false});

            if (readMethod != null)
                _readFunc = DelegateConverter.CompileFunc<Func<PortableReaderImpl, object, PortableCollectionInfo, object>>(null, readMethod, 
                    null, new[] {false, true, false, false});

            if (ctorInfo != null)
                _ctor = DelegateConverter.CompileCtor<Func<object, object>>(ctorInfo, new[] {typeof (int)});
        }

        /**
         * <summary>Generic dictionary flag.</summary>
         */
        public bool IsGenericDictionary
        {
            get { return _flag == FlagGenericDictionary; }
        }

        /**
         * <summary>Generic collection flag.</summary>
         */
        public bool IsGenericCollection
        {
            get { return _flag == FlagGenericCollection; }
        }

        /**
         * <summary>Dictionary flag.</summary>
         */
        public bool IsDictionary
        {
            get { return _flag == FlagDictionary; }
        }

        /**
         * <summary>Collection flag.</summary>
         */
        public bool IsCollection
        {
            get { return _flag == FlagCollection; }
        }

        /**
         * <summary>Whether at least one flag is set..</summary>
         */
        public bool IsAny
        {
            get { return _flag != FlagNone; }
        }

        /**
         * <summary>Write handler.</summary>
         */
        public PortableSystemWriteDelegate WriteHandler
        {
            get { return _writeHnd; }
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

            return _readFunc(reader, null, this);
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
