/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Config
{
    using System;

    /// <summary>
    /// Parses grid config values.
    /// </summary>
    internal class GridConfigValueParser
    {
        /// <summary>
        /// Parses provided string to int. Throws a custom exception if failed.
        /// </summary>
        public static int ParseInt(string value, string propertyName)
        {
            int result;

            if (int.TryParse(value, out result))
                return result;

            throw new InvalidOperationException(
                string.Format("Failed to configure Grid: property '{0}' has value '{1}', which is not an integer.",
                    propertyName, value));
        }
    }
}
