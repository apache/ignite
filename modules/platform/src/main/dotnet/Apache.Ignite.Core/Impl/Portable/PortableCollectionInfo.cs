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
    using System.Collections.Generic;
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
            new PortableCollectionInfo(FlagNone, null, null, null);

        /** Cache "dictionary" value. */
        private static readonly PortableCollectionInfo Dictionary =
            new PortableCollectionInfo(FlagDictionary, PortableSystemHandlers.WriteHndDictionary, null, null);

        /** Cache "collection" value. */
        private static readonly PortableCollectionInfo Collection =
            new PortableCollectionInfo(FlagCollection, PortableSystemHandlers.WriteHndCollection, null, null);

        /** Cached infos. */
        private static readonly IDictionary<Type, PortableCollectionInfo> Infos =
            new ConcurrentDictionary<Type, PortableCollectionInfo>(64, 32);

        /**
         * <summary>Get collection info for type.</summary>
         * <param name="type">Type.</param>
         * <returns>Collection info.</returns>
         */
        public static PortableCollectionInfo Info(Type type)
        {
            PortableCollectionInfo info;

            if (!Infos.TryGetValue(type, out info))
            {
                info = Info0(type);

                Infos[type] = info;
            }

            return info;
        }

        /**
         * <summary>Internal routine to get collection info for type.</summary>
         * <param name="type">Type.</param>
         * <returns>Collection info.</returns>
         */
        private static PortableCollectionInfo Info0(Type type)
        {
            if (type.IsGenericType)
            {
                if (type.GetGenericTypeDefinition() == PortableUtils.TypGenericDictionary)
                {
                    MethodInfo writeMthd =
                        PortableUtils.MtdhWriteGenericDictionary.MakeGenericMethod(type.GetGenericArguments());
                    MethodInfo readMthd =
                        PortableUtils.MtdhReadGenericDictionary.MakeGenericMethod(type.GetGenericArguments());

                    return new PortableCollectionInfo(FlagGenericDictionary,
                        PortableSystemHandlers.WriteHndGenericDictionary, writeMthd, readMthd);
                }

                Type genTyp = type.GetInterface(PortableUtils.TypGenericDictionary.FullName);

                if (genTyp != null)
                {
                    MethodInfo writeMthd =
                        PortableUtils.MtdhWriteGenericDictionary.MakeGenericMethod(genTyp.GetGenericArguments());
                    MethodInfo readMthd =
                        PortableUtils.MtdhReadGenericDictionary.MakeGenericMethod(genTyp.GetGenericArguments());

                    return new PortableCollectionInfo(FlagGenericDictionary,
                        PortableSystemHandlers.WriteHndGenericDictionary, writeMthd, readMthd);
                }

                if (type.GetGenericTypeDefinition() == PortableUtils.TypGenericCollection)
                {
                    MethodInfo writeMthd =
                        PortableUtils.MtdhWriteGenericCollection.MakeGenericMethod(type.GetGenericArguments());
                    MethodInfo readMthd =
                        PortableUtils.MtdhReadGenericCollection.MakeGenericMethod(type.GetGenericArguments());

                    return new PortableCollectionInfo(FlagGenericCollection,
                        PortableSystemHandlers.WriteHndGenericCollection, writeMthd, readMthd);
                }

                genTyp = type.GetInterface(PortableUtils.TypGenericCollection.FullName);

                if (genTyp != null)
                {
                    MethodInfo writeMthd =
                        PortableUtils.MtdhWriteGenericCollection.MakeGenericMethod(genTyp.GetGenericArguments());
                    MethodInfo readMthd =
                        PortableUtils.MtdhReadGenericCollection.MakeGenericMethod(genTyp.GetGenericArguments());

                    return new PortableCollectionInfo(FlagGenericCollection,
                        PortableSystemHandlers.WriteHndGenericCollection, writeMthd, readMthd);
                }
            }

            if (type == PortableUtils.TypDictionary || type.GetInterface(PortableUtils.TypDictionary.FullName) != null)
                return Dictionary;
            if (type == PortableUtils.TypCollection || type.GetInterface(PortableUtils.TypCollection.FullName) != null)
                return Collection;
            return None;
        }

        /** Flag. */
        private readonly byte _flag;

        /** Write handler. */
        private readonly PortableSystemWriteDelegate _writeHnd;

        /** Generic write func. */
        private readonly Action<object, PortableWriterImpl> _writeFunc;

        /** Generic read func. */
        private readonly Func<PortableReaderImpl, object, object> _readFunc;

        /**
         * <summary>Constructor.</summary>
         * <param name="flag0">Flag.</param>
         * <param name="writeHnd0">Write handler.</param>
         * <param name="writeMthd0">Generic write method.</param>
         * <param name="readMthd0">Generic read method.</param>
         */
        private PortableCollectionInfo(byte flag0, PortableSystemWriteDelegate writeHnd0,
            MethodInfo writeMthd0, MethodInfo readMthd0)
        {
            _flag = flag0;
            _writeHnd = writeHnd0;

            if (writeMthd0 != null)
                _writeFunc = DelegateConverter.CompileFunc<Action<object, PortableWriterImpl>>(null, writeMthd0, null,
                    new[] {true, false, false});

            if (readMthd0 != null)
                _readFunc = DelegateConverter.CompileFunc<Func<PortableReaderImpl, object, object>>(null, readMthd0, 
                    null, new[] {false, true, false});
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
        /// Reads the generic collection.
        /// </summary>
        public object ReadGeneric(PortableReaderImpl reader)
        {
            Debug.Assert(reader != null);
            Debug.Assert(_readFunc != null);

            return _readFunc(reader, null);
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
