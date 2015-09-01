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
    using System;
    using System.Runtime.InteropServices;
    
    using UU = UnmanagedUtils;

    /// <summary>
    /// Base unmanaged target implementation.
    /// </summary>
    internal unsafe sealed class UnmanagedTarget : CriticalHandle, IUnmanagedTarget
    {
        /** Context. */
        private readonly UnmanagedContext ctx;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ctx">Context.</param>
        /// <param name="target">Target.</param>
        public UnmanagedTarget(UnmanagedContext ctx, void* target)
            : base(IntPtr.Zero)
        {
            this.ctx = ctx;
            
            SetHandle(new IntPtr(target));
        }

        /** <inheritdoc /> */
        public void* Context
        {
            get { return ctx.NativeContext; }
        }

        /** <inheritdoc /> */
        public void* Target
        {
            get { return handle.ToPointer(); }
        }

        /** <inheritdoc /> */
        public IUnmanagedTarget ChangeTarget(void* target)
        {
            return new UnmanagedTarget(ctx, target);
        }

        /** <inheritdoc /> */
        protected override bool ReleaseHandle()
        {
            UU.Release(this);
            
            return true;
        }

        /** <inheritdoc /> */
        public override bool IsInvalid
        {
            get { return handle == IntPtr.Zero; }
        }
    }
}
