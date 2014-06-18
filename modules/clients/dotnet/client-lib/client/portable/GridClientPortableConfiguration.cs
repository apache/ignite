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
    using System.Collections.Generic;

    /**
     * <summary>Portable configuration.</summary>
     */ 
    class GridClientPortableConfiguration
    {
        /**
         * <summary>Constructor.</summary>
         */ 
        public GridClientPortableConfiguration()
        {
            // No-op.
        }

        /**
         * <summary>Copying constructor.</summary>
         * <param name="cfg">Configuration to copy.</param>
         */
        public GridClientPortableConfiguration(GridClientPortableConfiguration cfg)
        {
            ClassConfiguration = cfg.ClassConfiguration != null ? 
                new List<GridClientPortableClassConfiguration>(cfg.ClassConfiguration) : null;
        }

        /**
         * <summary>Portable classes configuration.</summary>
         */ 
        public ICollection<GridClientPortableClassConfiguration> ClassConfiguration
        {
            get;
            set;
        }
    }
}
