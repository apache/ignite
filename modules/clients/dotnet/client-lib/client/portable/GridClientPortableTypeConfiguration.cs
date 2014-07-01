/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable
{
    using System;

    /**
     * <summary>Portable type configuration.</summary>
     */ 
    public class GridClientPortableTypeConfiguration
    {
        /**
         * <summary>Constructor.</summary>
         */ 
        public GridClientPortableTypeConfiguration()
        {
            // No-op.
        }
        
        /**
         * <summary>Constructor.</summary>
         * <param name="type">Type.</param>
         */ 
        public GridClientPortableTypeConfiguration(Type type)
        {
            AssemblyName = type.Assembly.GetName().Name;
            AssemblyVersion = type.Assembly.GetName().Version.ToString();
            TypeName = type.FullName;
        }

        /**
         * <summary>Copying constructor.</summary>
         * <param name="cfg">Configuration to copy.</param>
         */
        public GridClientPortableTypeConfiguration(GridClientPortableTypeConfiguration cfg)
        {
            TypeName = cfg.TypeName;
            IdMapper = cfg.IdMapper;
            Serializer = cfg.Serializer;
        }

        /**
         * <summary>Assembly name.</summary>
         */ 
        public string AssemblyName
        {
            get;
            set;
        }

        /**
         * <summary>Assembly version.</summary>
         */
        public string AssemblyVersion
        {
            get;
            set;
        }

        /**
         * <summary>Fully qualified type name.</summary>
         */
        public string TypeName
        {
            get;
            set;
        }

        /**
         * <summary>ID mapper for the given class. When it is necessary to resolve class (field) ID, then 
         * this property will be checked first. If not set, then GridCLientPortableClassIdAttribute 
         * (GridClientPortableFieldIdAttribute) will be checked in class through reflection. If required
         * attribute is not set, then ID will be hash code of the class (field) simple name in lower case.</summary>
         */
        public GridClientPortableIdResolver IdMapper
        {
            get;
            set;
        }

        /**
         * <summary>Serializer for the given class. If not provided and class implements IGridClientPortable
         * then its custom logic will be used. If not provided and class doesn't implement IGridClientPortable
         * then all fields of the class except of those with [NotSerialized] attribute will be serialized
         * with help of reflection.</summary>
         */
        public IGridClientPortableSerializer Serializer
        {
            get;
            set;
        }

        /** {@inheritDoc} */
        override public String ToString()
        {
            return typeof(GridClientPortableTypeConfiguration).Name + " [assemblyName=" + AssemblyName + 
                ", assemblyVersion=" + AssemblyVersion + ", typeName=" + TypeName + ", IdMapper=" + IdMapper +
                ", Serializer=" + Serializer + ']';
        }
    }
}
