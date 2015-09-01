/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl
{
    /// <summary>
    /// Internal extensions for GridConfiguration.
    /// </summary>
    internal class GridConfigurationEx : GridConfiguration
    {
        /// <summary>
        /// Default constructor.
        /// </summary>
        public GridConfigurationEx()
        {
            // No-op.
        }

        /// <summary>
        /// Copying constructor.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        public GridConfigurationEx(GridConfiguration cfg) : base(cfg)
        {
            // No-op.
        }

        /// <summary>
        /// Copying constructor.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        public GridConfigurationEx(GridConfigurationEx cfg)
            : this((GridConfiguration)cfg)
        {
            GridName = cfg.GridName;
        }

        /// <summary>
        /// Grid name which is used if not provided in configuration file.
        /// </summary>
        public string GridName
        {
            get;
            set;
        }
    }
}
