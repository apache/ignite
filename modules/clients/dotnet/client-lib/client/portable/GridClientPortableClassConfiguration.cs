/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable
{
    /**
     * <summary>Portable class configuration.</summary>
     */ 
    public class GridClientPortableClassConfiguration
    {
        /**
         * <summary>Constructor.</summary>
         */ 
        public GridClientPortableClassConfiguration()
        {

        }

        /**
         * <summary>Constructor.</summary>
         * <param name="className">Class name.</param>
         */ 
        public GridClientPortableClassConfiguration(string className)
        {
            ClassName = className;
        }

        /**
         * <summary>Copying constructor.</summary>
         * <param name="cfg">Configuration to copy.</param>
         */
        public GridClientPortableClassConfiguration(GridClientPortableClassConfiguration cfg)
        {
            ClassName = cfg.ClassName;
            IdMapper = cfg.IdMapper;
            Serializer = cfg.Serializer;
        }

        /**
         * <summary>Fully qualified class name.</summary>
         */
        public string ClassName
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
        public string IdMapper
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
    }
}
