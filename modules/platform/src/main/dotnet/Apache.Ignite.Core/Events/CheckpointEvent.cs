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
    /// Grid checkpoint event.
    /// </summary>
    public sealed class CheckpointEvent : EventBase
	{
        /** */
        private readonly string key;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal CheckpointEvent(IPortableRawReader r) : base(r)
        {
            key = r.ReadString();
        }
		
        /// <summary>
        /// Gets checkpoint key associated with this event. 
        /// </summary>
        public string Key { get { return key; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: Key={1}", Name, Key);
	    }
    }
}
