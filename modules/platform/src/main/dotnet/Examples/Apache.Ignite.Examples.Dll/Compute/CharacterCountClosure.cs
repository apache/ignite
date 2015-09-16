/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using GridGain.Compute;

namespace GridGain.Examples.Compute
{
    /// <summary>
    /// Closure counting characters in a string.
    /// </summary>
    [Serializable]
    public class CharacterCountClosure : IComputeFunc<string, int>
    {
        /// <summary>
        /// Calculate character count of the given word.
        /// </summary>
        /// <param name="arg">Word.</param>
        /// <returns>Character count.</returns>
        public int Invoke(string arg)
        {
            int len = arg.Length;

            Console.WriteLine("Character count in word \"" + arg + "\": " + len);

            return len;
        }
    }
}
