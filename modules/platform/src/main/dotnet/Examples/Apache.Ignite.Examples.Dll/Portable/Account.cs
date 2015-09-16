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
    /// Account object. Used in transaction example.
    /// </summary>
    [Serializable]
    public class Account
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">Account ID.</param>
        /// <param name="balance">Account balance.</param>
        public Account(int id, decimal balance)
        {
            Id = id;
            Balance = balance;
        }
    
        /// <summary>
        /// Account ID.
        /// </summary>
        public int Id { get; set; }
    
        /// <summary>
        /// Account balance.
        /// </summary>
        public decimal Balance { get; set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        override public String ToString()
        {
            return string.Format("{0} [id={1}, balance={2}]", typeof(Account).Name, Id, Balance);
        }
    }
}
