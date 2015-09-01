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
    using GridGain.Portable;

	/// <summary>
    /// Grid swap space event.
    /// </summary>
    public sealed class SwapSpaceEvent : EventBase
	{
        /** */
        private readonly string space;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal SwapSpaceEvent(IPortableRawReader r) : base(r)
        {
            space = r.ReadString();
        }
		
        /// <summary>
        /// Gets swap space name. 
        /// </summary>
        public string Space { get { return space; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: Space={1}", Name, Space);
	    }
    }
}
