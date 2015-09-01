/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Events
{
    using System;
    
    using GridGain.Portable;

    /// <summary>
    /// Event reader.
    /// </summary>
    internal static class EventReader
    {
        /// <summary>
        /// Reads an event.
        /// </summary>
        /// <typeparam name="T">Type of the event</typeparam>
        /// <param name="reader">Reader.</param>
        /// <returns>Deserialized event.</returns>
        /// <exception cref="System.InvalidCastException">Incompatible event type.</exception>
        public static T Read<T>(IPortableReader reader) where T : IEvent
        {
            var r = reader.RawReader();

            var clsId = r.ReadInt();

            if (clsId == -1)
                return default(T);

            return (T) CreateInstance(clsId, r);
        }

        /// <summary>
        /// Creates an event instance by type id.
        /// </summary>
        /// <param name="clsId">Type id.</param>
        /// <param name="reader">Reader.</param>
        /// <returns>Created and deserialized instance.</returns>
        /// <exception cref="System.InvalidOperationException">Invalid event class id:  + clsId</exception>
        private static IEvent CreateInstance(int clsId, IPortableRawReader reader)
        {
            switch (clsId)
            {
                case 1: return new LicenseEvent(reader);
                case 2: return new CacheEvent(reader);
                case 3: return new CacheQueryExecutedEvent(reader);
                case 4: return new CacheQueryReadEvent(reader);
                case 5: return new CacheRebalancingEvent(reader);
                case 6: return new CheckpointEvent(reader);
                case 7: return new DiscoveryEvent(reader);
                case 8: return new JobEvent(reader);
                case 9: return new SwapSpaceEvent(reader);
                case 10: return new TaskEvent(reader);
            }

            throw new InvalidOperationException("Invalid event class id: " + clsId);
        }
    }
}