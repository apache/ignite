// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.store.hibernate;

import org.gridgain.examples.datagrid.store.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.product.*;
import org.hibernate.*;
import org.hibernate.cfg.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Example of {@link GridCacheStore} implementation that uses Hibernate
 * and deals with maps {@link UUID} to {@link Person}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class CacheHibernatePersonStore extends GridCacheStoreAdapter<Long, Person> {
    /** Default hibernate configuration resource path. */
    private static final String DFLT_HIBERNATE_CFG = "/org/gridgain/examples/datagrid/store/hibernate/hibernate.cfg.xml";

    /** Session attribute name. */
    private static final String ATTR_SES = "HIBERNATE_STORE_SESSION";

    /** Session factory. */
    private SessionFactory sesFactory;

    /**
     * Default constructor.
     */
    public CacheHibernatePersonStore() {
        sesFactory = new Configuration().configure(DFLT_HIBERNATE_CFG).buildSessionFactory();
    }

    /** {@inheritDoc} */
    @Override public Person load(@Nullable GridCacheTx tx, Long key) throws GridException {
        System.out.println("Store load [key=" + key + ", tx=" + tx + ']');

        Session ses = session(tx);

        try {
            return (Person) ses.get(Person.class, key);
        }
        catch (HibernateException e) {
            rollback(ses, tx);

            throw new GridException("Failed to load value from cache store with key: " + key, e);
        }
        finally {
            end(ses, tx);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, Long key, @Nullable Person val)
        throws GridException {
        System.out.println("Store put [key=" + key + ", val=" + val + ", tx=" + tx + ']');

        if (val == null) {
            remove(tx, key);

            return;
        }

        Session ses = session(tx);

        try {
            ses.saveOrUpdate(val);
        }
        catch (HibernateException e) {
            rollback(ses, tx);

            throw new GridException("Failed to put value to cache store [key=" + key + ", val" + val + "]", e);
        }
        finally {
            end(ses, tx);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"JpaQueryApiInspection"})
    @Override public void remove(@Nullable GridCacheTx tx, Long key) throws GridException {
        System.out.println("Store remove [key=" + key + ", tx=" + tx + ']');

        Session ses = session(tx);

        try {
            ses.createQuery("delete " + Person.class.getSimpleName() + " where key = :key")
                .setParameter("key", key).setFlushMode(FlushMode.ALWAYS).executeUpdate();
        }
        catch (HibernateException e) {
            rollback(ses, tx);

            throw new GridException("Failed to remove value from cache store with key: " + key, e);
        }
        finally {
            end(ses, tx);
        }
    }

    /**
     * Rolls back hibernate session.
     *
     * @param ses Hibernate session.
     * @param tx Cache ongoing transaction.
     */
    private void rollback(Session ses, GridCacheTx tx) {
        // Rollback only if there is no cache transaction,
        // otherwise txEnd() will do all required work.
        if (tx == null) {
            Transaction hTx = ses.getTransaction();

            if (hTx != null && hTx.isActive())
                hTx.rollback();
        }
    }

    /**
     * Ends hibernate session.
     *
     * @param ses Hibernate session.
     * @param tx Cache ongoing transaction.
     */
    private void end(Session ses, GridCacheTx tx) {
        // Commit only if there is no cache transaction,
        // otherwise txEnd() will do all required work.
        if (tx == null) {
            Transaction hTx = ses.getTransaction();

            if (hTx != null && hTx.isActive())
                hTx.commit();

            ses.close();
        }
    }

    /** {@inheritDoc} */
    @Override public void txEnd(GridCacheTx tx, boolean commit) throws GridException {
        Session ses = tx.removeMeta(ATTR_SES);

        if (ses != null) {
            Transaction hTx = ses.getTransaction();

            if (hTx != null) {
                try {
                    if (commit) {
                        ses.flush();

                        hTx.commit();
                    }
                    else
                        hTx.rollback();

                    System.out.println("Transaction ended [xid=" + tx.xid() + ", commit=" + commit + ']');
                }
                catch (HibernateException e) {
                    throw new GridException("Failed to end transaction [xid=" + tx.xid() +
                        ", commit=" + commit + ']', e);
                }
                finally {
                    ses.close();
                }
            }
        }
    }

    /**
     * Gets Hibernate session.
     *
     * @param tx Cache transaction.
     * @return Session.
     */
    private Session session(GridCacheTx tx) {
        Session ses;

        if (tx != null) {
            ses = tx.meta(ATTR_SES);

            if (ses == null) {
                ses = sesFactory.openSession();

                ses.beginTransaction();

                // Store session in transaction metadata, so it can be accessed
                // for other operations on the same transaction.
                tx.addMeta(ATTR_SES, ses);

                System.out.println("Hibernate session open [ses=" + ses + ", tx=" + tx.xid() + "]");
            }
        }
        else {
            ses = sesFactory.openSession();

            ses.beginTransaction();
        }

        return ses;
    }
}
