/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_FILTER_HPP
#define GRID_CLIENT_FILTER_HPP

#include <gridgain/gridclientpredicate.hpp>
#include <gridgain/gridclienttypedef.hpp>

/**
 * Template class for composite filters.
 */
template<class T>
class GridClientCompositeFilter: public GridClientPredicate<T> {
public:

    /** Public destructor. */
    virtual ~GridClientCompositeFilter() {
    }

    /**
     * Add a new predicate to the collection.
     *
     * @param pred The new predicate object.
     */
    virtual void add(const TGridClientNodePredicatePtr pred);

    /** Clears all filters. */
    virtual void clear();

    /**
     * Checks the given expression and returns true if the element has satisfied the condition. .
     *
     * @param e element to be checked.
     * @return true if the given element has meet the condition.
     */
    virtual bool apply(const T& e) const;
private:

    /** Typedef for the list of predicates. */
    typedef std::vector<TGridClientNodePredicatePtr> TPredicateList;

    /** The list of predicates. */
    TPredicateList filters;
};

/**
 * Add a new predicate to the collection.
 *
 * @param pred The new predicate object.
 */
template<class T>
inline void GridClientCompositeFilter<T>::add(const TGridClientNodePredicatePtr filter) {
    filters.push_back(filter);
}

/** Clears all filters. */
template<class T>
inline void GridClientCompositeFilter<T>::clear() {
    filters.clear();
}

/**
 * Checks the given expression and returns true if the element has satisfied the condition. .
 *
 * @param e element to be checked.
 * @return <tt>true</tt> if the given element has meet the condition.
 */
template<class T>
inline bool GridClientCompositeFilter<T>::apply(const T& e) const {
    bool res = true;

    for (size_t i = 0; res && i < filters.size(); ++i) {
        if (filters[i])
            res = filters[i]->apply(e);
    }

    return res;
}

#endif
