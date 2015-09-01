/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */


namespace GridGain.Impl.Cluster
{
    using GridGain.Cluster;
    using GridGain.Portable;

    /// <summary>
    /// 
    /// </summary>
    internal interface IClusterGroupEx : IClusterGroup
    {
        /// <summary>
        /// Gets protable metadata for type.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <returns>Metadata.</returns>
        IPortableMetadata Metadata(int typeId);
    }
}
