/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable
{
    using System.Collections.Generic;

    /**
     * <summary>Portable type configuration.</summary>
     */
    public class GridClientPortableConfiguration
    {
        /**
         * <summary>Constructor.</summary>
         */
        public GridClientPortableConfiguration()
        {
            DefaultIdMapper = new GridClientPortableReflectiveIdResolver();
            DefaultSerializer = new GridClientPortableReflectiveSerializer();
        }

        /**
         * <summary>Copying constructor.</summary>
         * <param name="cfg">Configuration to copy.</param>
         */
        public GridClientPortableConfiguration(GridClientPortableConfiguration cfg)
        {
            TypeConfigurations = cfg.TypeConfigurations;
            DefaultIdMapper = cfg.DefaultIdMapper;
            DefaultSerializer = cfg.DefaultSerializer;
        }

        /**
         * <summary>Type configurations.</summary>
         */ 
        public ICollection<GridClientPortableTypeConfiguration> TypeConfigurations
        {
            get;
            set;
        }

        /**
         * <summary>Default ID mapper.</summary>
         */ 
        public GridClientPortableIdResolver DefaultIdMapper
        {
            get;
            set;
        }

        /**
         * <summary>Default serializer.</summary>
         */ 
        public IGridClientPortableSerializer DefaultSerializer
        {
            get;
            set;
        }
    }
}
