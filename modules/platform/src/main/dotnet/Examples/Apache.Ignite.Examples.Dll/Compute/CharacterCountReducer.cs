/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using GridGain.Compute;

namespace GridGain.Examples.Compute
{
    /// <summary>
    /// Character count reducer which collects individual string lengths and aggregate them.
    /// </summary>
    public class CharacterCountReducer : IComputeReducer<int, int>
    {
        /// <summary> Total length. </summary>
        private int _length;

        /// <summary>
        /// Collect character counts of distinct words.
        /// </summary>
        /// <param name="res">Character count of a distinct word.</param>
        /// <returns><c>True</c> to continue collecting results until all closures are finished.</returns>
        public bool Collect(int res)
        {
            _length += res;

            return true;
        }

        /// <summary>
        /// Reduce all collected results.
        /// </summary>
        /// <returns>Total character count.</returns>
        public int Reduce()
        {
            return _length;
        }
    }
}
