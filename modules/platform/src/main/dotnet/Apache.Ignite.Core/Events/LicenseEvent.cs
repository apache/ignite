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
    /// Grid license event.
    /// </summary>
    public sealed class LicenseEvent : EventBase
	{
        /** */
        private readonly Guid licenseId;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal LicenseEvent(IPortableRawReader r) : base(r)
        {
            licenseId = r.ReadGuid() ?? Guid.Empty;
        }
		
        /// <summary>
        /// Gets license ID. 
        /// </summary>
        public Guid LicenseId { get { return licenseId; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: LicenseId={1}", Name, LicenseId);
	    }
    }
}
