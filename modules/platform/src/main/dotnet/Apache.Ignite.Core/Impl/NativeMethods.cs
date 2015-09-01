/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Native methods.
    /// </summary>
    internal static class NativeMethods
    {
        /// <summary>
        /// Load DLL with WinAPI.
        /// </summary>
        /// <param name="path">Path to dll.</param>
        /// <returns></returns>
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, 
            ThrowOnUnmappableChar = true)]
        internal static extern IntPtr LoadLibrary(string path);

        /// <summary>
        /// Get procedure address with WinAPI.
        /// </summary>
        /// <param name="ptr">DLL pointer.</param>
        /// <param name="name">Procedure name.</param>
        /// <returns>Procedure address.</returns>
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false, 
            ThrowOnUnmappableChar = true)]
        internal static extern IntPtr GetProcAddress(IntPtr ptr, string name);
    }
}