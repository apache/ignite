/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;

namespace GridGain.Examples.Portable
{
    /// <summary>
    /// Organization type.
    /// </summary>
    [Serializable]
    public enum OrganizationType
    {
        /// <summary>
        /// Non-profit organization.
        /// </summary>
        NonProfit,

        /// <summary>
        /// Private organization.
        /// </summary>
        Private,

        /// <summary>
        /// Government organization.
        /// </summary>
        Government
    }
}
