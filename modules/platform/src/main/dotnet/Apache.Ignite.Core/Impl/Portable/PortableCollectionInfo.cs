/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Portable
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
        private const byte FLAG_NONE = 0;

        /** Flag: generic dictionary. */
        private const byte FLAG_GENERIC_DICTIONARY = 1;

        /** Flag: generic collection. */
        private const byte FLAG_GENERIC_COLLECTION = 2;

        /** Flag: dictionary. */
        private const byte FLAG_DICTIONARY = 3;

        /** Flag: collection. */
        private const byte FLAG_COLLECTION = 4;

        /** Cache "none" value. */
        private static readonly PortableCollectionInfo NONE =
            new PortableCollectionInfo(FLAG_NONE, null, null, null);

        /** Cache "dictionary" value. */
        private static readonly PortableCollectionInfo DICTIONARY =
            new PortableCollectionInfo(FLAG_DICTIONARY, PortableSystemHandlers.WRITE_HND_DICTIONARY, null, null);

        /** Cache "collection" value. */
        private static readonly PortableCollectionInfo COLLECTION =
            new PortableCollectionInfo(FLAG_COLLECTION, PortableSystemHandlers.WRITE_HND_COLLECTION, null, null);

        /** Cached infos. */
        private static readonly IDictionary<Type, PortableCollectionInfo> INFOS =
            new ConcurrentDictionary<Type, PortableCollectionInfo>(64, 32);

        /**
         * <summary>Get collection info for type.</summary>
         * <param name="type">Type.</param>
         * <returns>Collection info.</returns>
         */
        public static PortableCollectionInfo Info(Type type)
        {
            PortableCollectionInfo info;

            if (!INFOS.TryGetValue(type, out info))
            {
                info = Info0(type);

                INFOS[type] = info;
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
                if (type.GetGenericTypeDefinition() == PortableUtils.TYP_GENERIC_DICTIONARY)
                {
                    MethodInfo writeMthd =
                        PortableUtils.MTDH_WRITE_GENERIC_DICTIONARY.MakeGenericMethod(type.GetGenericArguments());
                    MethodInfo readMthd =
                        PortableUtils.MTDH_READ_GENERIC_DICTIONARY.MakeGenericMethod(type.GetGenericArguments());

                    return new PortableCollectionInfo(FLAG_GENERIC_DICTIONARY,
                        PortableSystemHandlers.WRITE_HND_GENERIC_DICTIONARY, writeMthd, readMthd);
                }

                Type genTyp = type.GetInterface(PortableUtils.TYP_GENERIC_DICTIONARY.FullName);

                if (genTyp != null)
                {
                    MethodInfo writeMthd =
                        PortableUtils.MTDH_WRITE_GENERIC_DICTIONARY.MakeGenericMethod(genTyp.GetGenericArguments());
                    MethodInfo readMthd =
                        PortableUtils.MTDH_READ_GENERIC_DICTIONARY.MakeGenericMethod(genTyp.GetGenericArguments());

                    return new PortableCollectionInfo(FLAG_GENERIC_DICTIONARY,
                        PortableSystemHandlers.WRITE_HND_GENERIC_DICTIONARY, writeMthd, readMthd);
                }

                if (type.GetGenericTypeDefinition() == PortableUtils.TYP_GENERIC_COLLECTION)
                {
                    MethodInfo writeMthd =
                        PortableUtils.MTDH_WRITE_GENERIC_COLLECTION.MakeGenericMethod(type.GetGenericArguments());
                    MethodInfo readMthd =
                        PortableUtils.MTDH_READ_GENERIC_COLLECTION.MakeGenericMethod(type.GetGenericArguments());

                    return new PortableCollectionInfo(FLAG_GENERIC_COLLECTION,
                        PortableSystemHandlers.WRITE_HND_GENERIC_COLLECTION, writeMthd, readMthd);
                }

                genTyp = type.GetInterface(PortableUtils.TYP_GENERIC_COLLECTION.FullName);

                if (genTyp != null)
                {
                    MethodInfo writeMthd =
                        PortableUtils.MTDH_WRITE_GENERIC_COLLECTION.MakeGenericMethod(genTyp.GetGenericArguments());
                    MethodInfo readMthd =
                        PortableUtils.MTDH_READ_GENERIC_COLLECTION.MakeGenericMethod(genTyp.GetGenericArguments());

                    return new PortableCollectionInfo(FLAG_GENERIC_COLLECTION,
                        PortableSystemHandlers.WRITE_HND_GENERIC_COLLECTION, writeMthd, readMthd);
                }
            }

            if (type == PortableUtils.TYP_DICTIONARY || type.GetInterface(PortableUtils.TYP_DICTIONARY.FullName) != null)
                return DICTIONARY;
            else if (type == PortableUtils.TYP_COLLECTION || type.GetInterface(PortableUtils.TYP_COLLECTION.FullName) != null)
                return COLLECTION;
            else
                return NONE;
        }

        /** Flag. */
        private readonly byte flag;

        /** Write handler. */
        private readonly PortableSystemWriteDelegate writeHnd;

        /** Generic write func. */
        private readonly Action<object, PortableWriterImpl> writeFunc;

        /** Generic read func. */
        private readonly Func<PortableReaderImpl, object, object> readFunc;

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
            flag = flag0;
            writeHnd = writeHnd0;

            if (writeMthd0 != null)
                writeFunc = DelegateConverter.CompileFunc<Action<object, PortableWriterImpl>>(null, writeMthd0, null,
                    new[] {true, false, false});

            if (readMthd0 != null)
                readFunc = DelegateConverter.CompileFunc<Func<PortableReaderImpl, object, object>>(null, readMthd0, 
                    null, new[] {false, true, false});
        }

        /**
         * <summary>Generic dictionary flag.</summary>
         */
        public bool IsGenericDictionary
        {
            get { return flag == FLAG_GENERIC_DICTIONARY; }
        }

        /**
         * <summary>Generic collection flag.</summary>
         */
        public bool IsGenericCollection
        {
            get { return flag == FLAG_GENERIC_COLLECTION; }
        }

        /**
         * <summary>Dictionary flag.</summary>
         */
        public bool IsDictionary
        {
            get { return flag == FLAG_DICTIONARY; }
        }

        /**
         * <summary>Collection flag.</summary>
         */
        public bool IsCollection
        {
            get { return flag == FLAG_COLLECTION; }
        }

        /**
         * <summary>Whether at least one flag is set..</summary>
         */
        public bool IsAny
        {
            get { return flag != FLAG_NONE; }
        }

        /**
         * <summary>Write handler.</summary>
         */
        public PortableSystemWriteDelegate WriteHandler
        {
            get { return writeHnd; }
        }

        /// <summary>
        /// Reads the generic collection.
        /// </summary>
        public object ReadGeneric(PortableReaderImpl reader)
        {
            Debug.Assert(reader != null);
            Debug.Assert(readFunc != null);

            return readFunc(reader, null);
        }

        /// <summary>
        /// Writes the generic collection.
        /// </summary>
        public void WriteGeneric(PortableWriterImpl writer, object value)
        {
            Debug.Assert(writer != null);
            Debug.Assert(writeFunc != null);

            writeFunc(value, writer);
        }
    }
}
