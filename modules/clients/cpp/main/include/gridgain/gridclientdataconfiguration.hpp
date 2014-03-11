/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTDATACONFIGURATION_HPP_
#define GRIDCLIENTDATACONFIGURATION_HPP_

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclientdataaffinity.hpp>

#include <string>
#include <memory>

/**
 * Client cache configuration class.
 */
class GRIDGAIN_API GridClientDataConfiguration {
public:
    /**
     * Default constructor.
     */
    GridClientDataConfiguration();

    /**
     * Copy constructor.
     */
    GridClientDataConfiguration(const GridClientDataConfiguration& cfg);

    /**
     * Assignment operator.
     *
     * @param right Right hand side object to copy data from.
     * @return This object for chaining.
     */
    GridClientDataConfiguration& operator=(const GridClientDataConfiguration& right);

    /**
     * Destructor.
     */
    ~GridClientDataConfiguration();

    /**
     * Gets cache name.
     *
     * @return Cache name.
     */
    std::string name() const;

    /**
     * Sets cache name.
     *
     * @param name Cache name.
     */
    void name(const std::string& name);

    /**
     * Gets cache affinity.
     *
     * @return Cache affinity.
     */
    std::shared_ptr<GridClientDataAffinity> affinity();

    /**
     * Sets cache affinity.
     *
     * @param aff Cache affinity.
     */
    void affinity(const std::shared_ptr<GridClientDataAffinity>& aff);

private:
    /** Implementation. */
    class Impl;
    Impl* pimpl;
};

#endif /* GRIDCLIENTDATACONFIGURATION_HPP_ */
