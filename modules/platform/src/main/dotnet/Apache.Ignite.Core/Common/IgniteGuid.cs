/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Common
{
    using System;
    using GridGain.Portable;

    /// <summary>
    /// Grid guid with additional local ID.
    /// </summary>
    public struct GridGuid : IEquatable<GridGuid>
    {
        /** Global id. */
        private readonly Guid globalId;

        /** Local id. */
        private readonly long localId;

        /// <summary>
        /// Initializes a new instance of the <see cref="GridGuid"/> struct.
        /// </summary>
        /// <param name="globalId">The global id.</param>
        /// <param name="localId">The local id.</param>
        public GridGuid(Guid globalId, long localId)
        {
            this.globalId = globalId;
            this.localId = localId;
        }

        /// <summary>
        /// Gets the global id.
        /// </summary>
        public Guid GlobalId
        {
            get { return globalId; }
        }

        /// <summary>
        /// Gets the local id.
        /// </summary>
        public long LocalId
        {
            get { return localId; }
        }

        /** <inheritDoc /> */
        public bool Equals(GridGuid other)
        {
            return globalId.Equals(other.globalId) && localId == other.localId;
        }

        /** <inheritDoc /> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is GridGuid && Equals((GridGuid) obj);
        }

        /** <inheritDoc /> */
        public override int GetHashCode()
        {
            unchecked
            {
                return (globalId.GetHashCode() * 397) ^ localId.GetHashCode();
            }
        }

        /** <inheritDoc /> */
        public override string ToString()
        {
            return string.Format("GridGuid [GlobalId={0}, LocalId={1}]", GlobalId, LocalId);
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary> 
        /// <param name="w">Writer.</param>
        public void WritePortable(IPortableRawWriter w)
        {
            w.WriteGuid(GlobalId);
            w.WriteLong(LocalId);
        }

        /// <summary>
        /// Reads this object from the given reader.
        /// </summary> 
        /// <param name="r">Reader.</param>
        public static GridGuid ReadPortable(IPortableRawReader r)
        {
            var guid = r.ReadGuid();

            return guid == null
                ? new GridGuid(Guid.Empty, 0)
                : new GridGuid(guid.Value, r.ReadLong());
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        /// <param name="a">First item.</param>
        /// <param name="b">Second item.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator ==(GridGuid a, GridGuid b)
        {
            return a.Equals(b);
        }

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        /// <param name="a">First item.</param>
        /// <param name="b">Second item.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static bool operator !=(GridGuid a, GridGuid b)
        {
            return !(a == b);
        }
    }
}