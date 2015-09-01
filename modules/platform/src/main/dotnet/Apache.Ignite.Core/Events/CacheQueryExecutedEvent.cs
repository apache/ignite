/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Events
{
    using System;

    using GridGain.Portable;
    
	/// <summary>
    /// Cache query execution event.
    /// </summary>
    public sealed class CacheQueryExecutedEvent : EventBase
	{
        /** */
        private readonly string queryType;

        /** */
        private readonly string cacheName;

        /** */
        private readonly string className;

        /** */
        private readonly string clause;

        /** */
        private readonly Guid subjectId;

        /** */
        private readonly string taskName;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        internal CacheQueryExecutedEvent(IPortableRawReader r) : base(r)
        {
            queryType = r.ReadString();
            cacheName = r.ReadString();
            className = r.ReadString();
            clause = r.ReadString();
            subjectId = r.ReadGuid() ?? Guid.Empty;
            taskName = r.ReadString();
        }
		
        /// <summary>
        /// Gets query type. 
        /// </summary>
        public string QueryType { get { return queryType; } }

        /// <summary>
        /// Gets cache name on which query was executed. 
        /// </summary>
        public string CacheName { get { return cacheName; } }

        /// <summary>
        /// Gets queried class name. Applicable for SQL and full text queries. 
        /// </summary>
        public string ClassName { get { return className; } }

        /// <summary>
        /// Gets query clause. Applicable for SQL, SQL fields and full text queries. 
        /// </summary>
        public string Clause { get { return clause; } }

        /// <summary>
        /// Gets security subject ID. 
        /// </summary>
        public Guid SubjectId { get { return subjectId; } }

        /// <summary>
        /// Gets the name of the task that executed the query (if any). 
        /// </summary>
        public string TaskName { get { return taskName; } }

        /** <inheritDoc /> */
	    public override string ToShortString()
	    {
	        return string.Format("{0}: QueryType={1}, CacheName={2}, ClassName={3}, Clause={4}, SubjectId={5}, " +
	                             "TaskName={6}", Name, QueryType, CacheName, ClassName, Clause, SubjectId, TaskName);
	    }
    }
}
