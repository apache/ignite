/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Compute
{
    /// <summary>
    /// Defines function having a single argument.
    /// </summary>
    public interface IComputeFunc<in T, out R>
    {
        /// <summary>
        /// Invoke function.
        /// </summary>
        /// <param name="arg">Argument.</param>
        /// <returns>Result.</returns>
        R Invoke(T arg);
    }

    /// <summary>
    /// Defines function having no arguments.
    /// </summary>
    public interface IComputeFunc<out T>
    {
        /// <summary>
        /// Invoke function.
        /// </summary>
        /// <returns>Result.</returns>
        T Invoke();
    }

    /// <summary>
    /// Defines a void function having no arguments.
    /// </summary>
    public interface IComputeAction
    {
        /// <summary>
        /// Invokes action.
        /// </summary>
        void Invoke();
    }
}
