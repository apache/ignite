/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

/**
 * @file
 * Declares ignite::transactions::TransactionMetrics class.
 */

#ifndef _IGNITE_TRANSACTIONS_TRANSACTION_METRICS
#define _IGNITE_TRANSACTIONS_TRANSACTION_METRICS

#include <stdint.h>

#include <ignite/timestamp.h>

namespace ignite
{
    namespace transactions
    {
        /**
         * %Transaction metrics, shared across all caches.
         */
        class IGNITE_IMPORT_EXPORT TransactionMetrics
        {
        public:
            /**
             * Default constructor.
             *
             * Constructed instance is not valid.
             */
            TransactionMetrics() :
                valid(false),
                commitTime(),
                rollbackTime(),
                commits(),
                rollbacks()
            {
                // No-op.
            }

            /**
             * Constructor.
             *
             * @param commitTime The last time transaction was committed.
             * @param rollbackTime The last time transaction was rolled back.
             * @param commits The total number of transaction commits.
             * @param rollbacks The total number of transaction rollbacks.
             */
            TransactionMetrics(const Timestamp& commitTime,
                const Timestamp& rollbackTime, int32_t commits, int32_t rollbacks) :
                valid(true),
                commitTime(commitTime),
                rollbackTime(rollbackTime),
                commits(commits),
                rollbacks(rollbacks)
            {
                //No-op.
            }

            /**
             * Copy constructor.
             *
             * @param other Another instance.
             */
            TransactionMetrics(const TransactionMetrics& other) :
                valid(other.valid),
                commitTime(other.commitTime),
                rollbackTime(other.rollbackTime),
                commits(other.commits),
                rollbacks(other.rollbacks)
            {
                // No-op.
            }

            /**
             * Assignment operator.
             *
             * @param other Another instance.
             * @return @c *this.
             */
            TransactionMetrics& operator=(const TransactionMetrics& other)
            {
                valid = other.valid;
                commitTime = other.commitTime;
                rollbackTime = other.rollbackTime;
                commits = other.commits;
                rollbacks = other.rollbacks;

                return *this;
            }

            /**
             * Get commit time.
             *
             * @return The last time transaction was committed.
             */
            const Timestamp& GetCommitTime() const
            {
                return commitTime;
            }

            /**
             * Get rollback time.
             *
             * @return The last time transaction was rolled back.
             */
            const Timestamp& GetRollbackTime() const
            {
                return rollbackTime;
            }

            /**
             * Get the total number of transaction commits.
             *
             * @return The total number of transaction commits.
             */
            int32_t GetCommits() const
            {
                return commits;
            }

            /**
             * Get the total number of transaction rollbacks.
             *
             * @return The total number of transaction rollbacks.
             */
            int32_t GetRollbacks() const
            {
                return rollbacks;
            }

            /**
             * Check wheather the instance is valid.
             *
             * Invalid instance can be returned if some of the previous
             * operations have resulted in a failure. For example invalid
             * instance can be returned by not-throwing version of method
             * in case of error. Invalid instances also often can be
             * created using default constructor.
             *
             * @return @c true if the instance contains valid data.
             */
            bool IsValid() const
            {
                return valid;
            }

        private:
            /** Wheather instance is valid. */
            bool valid;

            /** The last time transaction was committed. */
            Timestamp commitTime;

            /** The last time transaction was rolled back. */
            Timestamp rollbackTime;

            /** The total number of transaction commits. */
            int32_t commits;

            /** The total number of transaction rollbacks. */
            int32_t rollbacks;
        };
    }
}

#endif //_IGNITE_TRANSACTIONS_TRANSACTION_METRICS