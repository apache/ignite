/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Events
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using GridGain.Events;
    using GridGain.Impl.Common;
    using GridGain.Impl.Portable;
    
    /// <summary>
    /// Event filter/listener holder for RemoteListen.
    /// </summary>
    internal class RemoteListenEventFilter : IInteropCallback
    {
        /** */
        private readonly GridImpl grid;
        
        /** */
        private readonly Func<Guid, IEvent, bool> filter;

        /// <summary>
        /// Initializes a new instance of the <see cref="RemoteListenEventFilter"/> class.
        /// </summary>
        /// <param name="grid">The grid.</param>
        /// <param name="filter">The filter.</param>
        public RemoteListenEventFilter(GridImpl grid, Func<Guid, IEvent, bool> filter)
        {
            this.grid = grid;
            this.filter = filter;
        }

        /** <inheritdoc /> */
        public int Invoke(IPortableStream stream)
        {
            var reader = grid.Marshaller.StartUnmarshal(stream);

            var evt = EventReader.Read<IEvent>(reader);

            var nodeId = reader.ReadGuid() ?? Guid.Empty;

            return filter(nodeId, evt) ? 1 : 0;
        }

        /// <summary>
        /// Creates an instance of this class from a stream.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <param name="grid">Grid</param>
        /// <returns>Deserialized instance of <see cref="RemoteListenEventFilter"/></returns>
        public static RemoteListenEventFilter CreateInstance(long memPtr, GridImpl grid)
        {
            Debug.Assert(grid != null);

            using (var stream = GridManager.Memory.Get(memPtr).Stream())
            {
                var marsh = grid.Marshaller;

                var reader = marsh.StartUnmarshal(stream);

                var pred = reader.ReadObject<PortableOrSerializableObjectHolder>().Item;

                var func = DelegateTypeDescriptor.GetEventFilter(pred.GetType());

                return new RemoteListenEventFilter(grid, (id, evt) => func(pred, id, evt));
            }
        }
    }
}