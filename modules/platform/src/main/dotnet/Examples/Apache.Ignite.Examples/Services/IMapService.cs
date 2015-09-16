/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Examples.Services
{
    /// <summary>
    /// Interface for service proxy interaction.
    /// Actual service class (<see cref="MapService{TK, TV}"/>) does not have to implement this interface. 
    /// Target method/property will be searched by signature (name, arguments).
    /// </summary>
    public interface IMapService<TK, TV>
    {
        /// <summary>
        /// Puts an entry to the map.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        void Put(TK key, TV value);

        /// <summary>
        /// Gets an entry from the map.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>Entry value.</returns>
        TV Get(TK key);

        /// <summary>
        /// Clears the map.
        /// </summary>
        void Clear();

        /// <summary>
        /// Gets the size of the map.
        /// </summary>
        /// <value>
        /// The size.
        /// </value>
        int Size { get; }
    }
}