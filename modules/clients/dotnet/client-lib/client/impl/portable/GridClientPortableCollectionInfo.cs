/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Portable
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Reflection;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;
    using PSH = GridGain.Client.Impl.Portable.GridClientPortableSystemHandlers;

    /**
     * <summary>Collection info helper.</summary>
     */ 
    internal struct GridClientPortableCollectionInfo
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
        private static GridClientPortableCollectionInfo NONE = new GridClientPortableCollectionInfo(FLAG_NONE, null, null, null);

        /** Cache "dictionary" value. */
        private static GridClientPortableCollectionInfo DICTIONARY = new GridClientPortableCollectionInfo(FLAG_DICTIONARY, PSH.WriteDictionary, null, null);

        /** Cache "collection" value. */
        private static GridClientPortableCollectionInfo COLLECTION = new GridClientPortableCollectionInfo(FLAG_COLLECTION, PSH.WriteCollection, null, null);

        /** Cached infos. */
        private static IDictionary<Type, GridClientPortableCollectionInfo> INFOS = new ConcurrentDictionary<Type, GridClientPortableCollectionInfo>();

        /**
         * <summary>Get collection info for type.</summary>
         * <param name="type">Type.</param>
         * <returns>Collection info.</returns>
         */ 
        public static GridClientPortableCollectionInfo Info(Type type)
        {
            GridClientPortableCollectionInfo info;

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
        private static GridClientPortableCollectionInfo Info0(Type type)
        {
            if (type.IsGenericType)
            {
                if (type.GetGenericTypeDefinition() == PU.TYP_GENERIC_DICTIONARY)
                {
                    MethodInfo writeMthd = PU.MTDH_WRITE_GENERIC_DICTIONARY.MakeGenericMethod(type.GetGenericArguments());
                    MethodInfo readMthd = PU.MTDH_READ_GENERIC_DICTIONARY.MakeGenericMethod(type.GetGenericArguments());

                    return new GridClientPortableCollectionInfo(FLAG_GENERIC_DICTIONARY, PSH.WriteGenericDictionary, writeMthd, readMthd);
                }

                Type genTyp = type.GetInterface(PU.TYP_GENERIC_DICTIONARY.FullName);

                if (genTyp != null)
                {
                    MethodInfo writeMthd = PU.MTDH_WRITE_GENERIC_DICTIONARY.MakeGenericMethod(genTyp.GetGenericArguments());
                    MethodInfo readMthd = PU.MTDH_READ_GENERIC_DICTIONARY.MakeGenericMethod(genTyp.GetGenericArguments());

                    return new GridClientPortableCollectionInfo(FLAG_GENERIC_DICTIONARY, PSH.WriteGenericDictionary, writeMthd, readMthd);
                }

                if (type.GetGenericTypeDefinition() == PU.TYP_GENERIC_COLLECTION)
                {
                    MethodInfo writeMthd = PU.MTDH_WRITE_GENERIC_COLLECTION.MakeGenericMethod(type.GetGenericArguments());
                    MethodInfo readMthd = PU.MTDH_READ_GENERIC_COLLECTION.MakeGenericMethod(type.GetGenericArguments());

                    return new GridClientPortableCollectionInfo(FLAG_GENERIC_COLLECTION, PSH.WriteGenericCollection, writeMthd, readMthd);
                }

                genTyp = type.GetInterface(PU.TYP_GENERIC_COLLECTION.FullName);

                if (genTyp != null)
                {
                    MethodInfo writeMthd = PU.MTDH_WRITE_GENERIC_COLLECTION.MakeGenericMethod(genTyp.GetGenericArguments());
                    MethodInfo readMthd = PU.MTDH_READ_GENERIC_COLLECTION.MakeGenericMethod(genTyp.GetGenericArguments());

                    return new GridClientPortableCollectionInfo(FLAG_GENERIC_COLLECTION, PSH.WriteGenericCollection, writeMthd, readMthd);
                }              
            }

            if (type == PU.TYP_DICTIONARY || type.GetInterface(PU.TYP_DICTIONARY.FullName) != null)
                return DICTIONARY;
            else if (type == PU.TYP_COLLECTION || type.GetInterface(PU.TYP_COLLECTION.FullName) != null)
                return COLLECTION;
            else 
                return NONE;
        }

        /** Flag. */
        private byte flag;

        /** Write handler. */
        private GridClientPortableSystemWriteDelegate writeHnd;

        /** Generic write method. */
        private MethodInfo writeMthd;

        /** Generic read method. */
        private MethodInfo readMthd;

        /**
         * <summary>Constructor.</summary>
         * <param name="flag0">Flag.</param>
         * <param name="writeHnd0">Write handler.</param>
         * <param name="writeMthd0">Generic write method.</param>
         * <param name="readMthd0">Generic read method.</param>
         */
        private GridClientPortableCollectionInfo(byte flag0, GridClientPortableSystemWriteDelegate writeHnd0, MethodInfo writeMthd0, MethodInfo readMthd0)
        {
            flag = flag0;
            writeHnd = writeHnd0;
            writeMthd = writeMthd0;
            readMthd = readMthd0;
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
        public GridClientPortableSystemWriteDelegate WriteHandler
        {
            get { return writeHnd; }
        }

        /**
         * <summary>Generic write method.</summary>
         */ 
        public MethodInfo GenericWriteMethod
        {
            get { return writeMthd; }
        }

        /**
         * <summary>Generic read method.</summary>
         */
        public MethodInfo GenericReadMethod
        {
            get { return readMthd; }
        }
    }
}
