// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <iostream>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "gridgain/gridclientdatametrics.hpp"

static int64_t currentTimeInMills() {
    boost::posix_time::ptime time = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration duration(time.time_of_day());

    return duration.total_milliseconds();
}

class GridClientDataMetrics::Impl {
public:
    Impl(){
        createTime_ = readTime_ = writeTime_ = (long) currentTimeInMills();

        reads_ = 0; writes_ = 0;
        hits_ = 0; misses_ = 0;
    }

    Impl(const Impl& other): createTime_(other.createTime_), readTime_(other.readTime_),
        writeTime_(other.writeTime_), reads_ (other.reads_), writes_ (other.writes_), hits_ (other.hits_),
        misses_ (other.misses_) {
    }

    /** Create time. */
    long createTime_;

    /** Last read time. */
    long readTime_;

    /** Last update time. */
    long writeTime_;

    /** Number of reads. */
    int reads_;

    /** Number of writes. */
    int writes_;

    /** Number of hits. */
    int hits_;

    /** Number of misses. */
    int misses_;
};


/** Default constructor. */
GridClientDataMetrics::GridClientDataMetrics() : pimpl(new Impl) {
}

/**
* Copy constructor.
*
* @param other Metrics to copy data from.
*/
GridClientDataMetrics::GridClientDataMetrics(const GridClientDataMetrics& other) 
    : pimpl(new Impl(*other.pimpl)){
}

GridClientDataMetrics& GridClientDataMetrics::operator=(const GridClientDataMetrics& rhs){
    if (this != &rhs) {
        delete pimpl;

        pimpl=new Impl(*rhs.pimpl);
    }

    return *this;
}

GridClientDataMetrics::~GridClientDataMetrics(){
    delete pimpl;
}

/**
* Gets create time of the owning entity (either cache or entry).
*
* @return Create time.
*/
long GridClientDataMetrics::createTime() const { 
    return pimpl->createTime_; 
}

/**
* Gets last write time of the owning entity (either cache or entry).
*
* @return Last write time.
*/
long GridClientDataMetrics::writeTime() const { 
    return pimpl->writeTime_; 
}

/**
* Gets last read time of the owning entity (either cache or entry).
*
* @return Last read time.
*/
long GridClientDataMetrics::readTime() const { 
    return pimpl->readTime_; 
}

/**
* Gets total number of reads of the owning entity (either cache or entry).
*
* @return Total number of reads.
*/
int GridClientDataMetrics::reads() const { 
    return pimpl->reads_; 
}

/**
* Gets total number of writes of the owning entity (either cache or entry).
*
* @return Total number of writes.
*/
int GridClientDataMetrics::writes() const { 
    return pimpl->writes_; 
}

/**
* Gets total number of hits for the owning entity (either cache or entry).
*
* @return Number of hits.
*/
int GridClientDataMetrics::hits() const { 
    return pimpl->hits_; 
}

/**
* Gets total number of misses for the owning entity (either cache or entry).
*
* @return Number of misses.
*/
int GridClientDataMetrics::misses() const { 
    return pimpl->misses_; 
}

/**
* Sets creation time.
*
* @param createTime Creation time.
*/
void GridClientDataMetrics::createTime(long createTime) {
    pimpl->createTime_ = createTime;
}

/**
    * Sets read time.
    *
    * @param readTime Read time.
    */
void GridClientDataMetrics::readTime(long readTime) {
    pimpl->readTime_ = readTime;
}

/**
* Sets write time.
*
* @param writeTime Write time.
*/
void GridClientDataMetrics::writeTime(long writeTime) {
    pimpl->writeTime_ = writeTime;
}

/**
* Sets number of reads.
*
* @param reads Number of reads.
*/
void GridClientDataMetrics::reads(int reads) {
    pimpl->reads_ = reads;
}

/**
* Sets number of writes.
*
* @param writes Number of writes.
*/
void GridClientDataMetrics::writes(int writes) {
    pimpl->writes_ = writes;
}

/**
* Sets number of hits.
*
* @param hits Number of hits.
*/
void GridClientDataMetrics::hits(int hits) {
    pimpl->hits_ = hits;
}

/**
* Sets number of misses.
*
* @param misses Number of misses.
*/
void GridClientDataMetrics::misses(const int& misses) {
    pimpl->misses_ = misses;
}
