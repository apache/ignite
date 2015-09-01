/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Services
{
    /// <summary>
    /// Represents grid-managed service.
    /// </summary>
    public interface IService
    {
        /// <summary>
        /// Initializes this instance before execution.
        /// </summary>
        /// <param name="context">Service execution context.</param>
        void Init(IServiceContext context);

        /// <summary>
        /// Starts execution of this service. This method is automatically invoked whenever an instance of the service
        /// is deployed on a grid node. Note that service is considered deployed even after it exits the Execute
        /// method and can be cancelled (or undeployed) only by calling any of the Cancel methods on 
        /// <see cref="IServices"/> API. Also note that service is not required to exit from Execute method until
        /// Cancel method was called.
        /// </summary>
        /// <param name="context">Service execution context.</param>
        void Execute(IServiceContext context);

        /// <summary>
        /// Cancels this instance.
        /// <para/>
        /// Note that grid cannot guarantee that the service exits from <see cref="IService.Execute"/>
        /// method whenever <see cref="IService.Cancel"/> is called. It is up to the user to
        /// make sure that the service code properly reacts to cancellations.
        /// </summary>
        /// <param name="context">Service execution context.</param>
        void Cancel(IServiceContext context);
    }
}