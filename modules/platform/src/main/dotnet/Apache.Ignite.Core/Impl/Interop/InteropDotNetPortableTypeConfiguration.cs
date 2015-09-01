
/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Interop
{
    using System;
    using System.Reflection;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// .Net portable configuration type as defined in Java configuration.
    /// </summary>
    internal class InteropDotNetPortableTypeConfiguration : IPortableWriteAware
    {
        /// <summary>
        /// Assembly name.
        /// </summary>
        public string AssemblyName { get; set; }

        /// <summary>
        /// Fully qualified type name.
        /// </summary>
        public string TypeName { get; set; }

        /// <summary>
        /// Name mapper for the given type.
        /// </summary>
        public string NameMapper { get; set; }

        /// <summary>
        /// ID mapper for the given type. When it is necessary to resolve class (field) ID, then
        /// this property will be checked first. If not set, then PortableClassIdAttribute
        /// (PortableFieldIdAttribute) will be checked in class through reflection. If required
        /// attribute is not set, then ID will be hash code of the class (field) simple name in lower case.
        /// </summary>
        public string IdMapper { get; set; }

        /// <summary>
        /// Serializer for the given type. If not provided and class implements IPortable
        /// then its custom logic will be used. If not provided and class doesn't implement IPortable
        /// then all fields of the class except of those with [NotSerialized] attribute will be serialized
        ///with help of reflection.
        /// </summary>
        public string Serializer { get; set; }

        /// <summary>
        /// Affinity key field name.
        /// </summary>
        public string AffinityKeyFieldName { get; set; }

        /// <summary>
        /// Metadata enabled flag. If set to non-null value, overrides default value set in
        /// PortableConfiguration.
        /// </summary>
        public bool? MetadataEnabled { get; set; }

        /// <summary>
        /// Keep deserialized flag. If set to non-null value, overrides default value set in 
        /// PortableConfiguration.
        /// </summary>
        public bool? KeepDeserialized { get; set; }

        /// <summary>
        /// Creates new instance of PortableTypeConfiguration.
        /// </summary>
        /// <returns>PortableTypeConfiguration</returns>
        public PortableTypeConfiguration ToPortableTypeConfiguration()
        {
            return new PortableTypeConfiguration
            {
                AssemblyName = AssemblyName,
                AffinityKeyFieldName = AffinityKeyFieldName,
                TypeName = TypeName,
                NameMapper = (IPortableNameMapper) CreateInstance(NameMapper),
                IdMapper = (IPortableIdMapper) CreateInstance(IdMapper),
                Serializer = (IPortableSerializer) CreateInstance(Serializer),
                MetadataEnabled = MetadataEnabled,
                KeepDeserialized = KeepDeserialized
            };
        }

        /** {@inheritDoc} */
        public void WritePortable(IPortableWriter writer)
        {
            IPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteString(AssemblyName);
            rawWriter.WriteString(TypeName);
            rawWriter.WriteString(NameMapper);
            rawWriter.WriteString(IdMapper);
            rawWriter.WriteString(Serializer);
            rawWriter.WriteString(AffinityKeyFieldName);
            rawWriter.WriteObject(MetadataEnabled);
            rawWriter.WriteObject(KeepDeserialized);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InteropDotNetPortableTypeConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public InteropDotNetPortableTypeConfiguration(IPortableReader reader)
        {
            IPortableRawReader rawReader = reader.RawReader();

            AssemblyName = rawReader.ReadString();
            TypeName = rawReader.ReadString();
            NameMapper = rawReader.ReadString();
            IdMapper = rawReader.ReadString();
            Serializer = rawReader.ReadString();
            AffinityKeyFieldName = rawReader.ReadString();
            MetadataEnabled = rawReader.ReadObject<bool?>();
            KeepDeserialized = rawReader.ReadObject<bool?>();
        }

        /// <summary>
        /// Create new instance of specified class.
        /// </summary>
        /// <param name="typeName">Name of the type.</param>
        /// <returns>New Instance.</returns>
        public static object CreateInstance(string typeName)
        {
            if (typeName == null)
                return null;

            foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                object instance = assembly.CreateInstance(typeName);

                if (instance != null)
                    return instance;
            }

            throw new PortableException("Failed to find class: " + typeName);
        }
    }
}
