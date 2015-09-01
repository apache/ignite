/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Unmanaged
{
    /// <summary>
    /// Unmanaged context.
    /// Wrapper around native ctx pointer to track finalization.
    /// </summary>
    internal unsafe class UnmanagedContext
    {
        /** Context */
        private readonly void* nativeCtx;

        /// <summary>
        /// Constructor.
        /// </summary>
        public UnmanagedContext(void* ctx)
        {
            nativeCtx = ctx;
        }

        /// <summary>
        /// Gets the native context pointer.
        /// </summary>
        public void* NativeContext
        {
            get { return nativeCtx; }
        }

        /// <summary>
        /// Destructor.
        /// </summary>
        ~UnmanagedContext()
        {
            UnmanagedUtils.DeleteContext(nativeCtx); // Release CPP object.
        }
    }
}