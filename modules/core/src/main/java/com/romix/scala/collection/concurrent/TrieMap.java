/*
 Copyright (C) Roman Levenstein. All Rights Reserved.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package com.romix.scala.collection.concurrent;

import com.romix.scala.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * This is a port of Scala's TrieMap class from the Scala Collections library.
 *
 * @param <K>
 * @param <V>
 * @author Roman Levenstein <romixlev@gmail.com>
 */
@SuppressWarnings({"unchecked", "rawtypes", "unused"})
public class TrieMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V> {
    /**
     * Only used for ctrie serialization.
     */
    // @SerialVersionUID(0L - 7237891413820527142L)
    private static long TrieMapSerializationEnd = 0L - 7237891413820527142L;
    /**
     * EntrySet
     */
    private final EntrySet entrySet = new EntrySet();
    AtomicReferenceFieldUpdater<INodeBase, MainNode> inodeupdater = AtomicReferenceFieldUpdater.newUpdater(INodeBase.class, MainNode.class, "mainnode");

    // static class MangledHashing<K> extends Hashing<K> {
    // int hash(K k) {
    // return util.hashing.byteswap32(k);
    // }
    // }
    private Object r;
    private AtomicReferenceFieldUpdater<TrieMap, Object> rtupd;
    private Hashing<K> hashf;
    private Equiv<K> ef;
    private Hashing<K> hashingobj;
    private Equiv<K> equalityobj;
    private AtomicReferenceFieldUpdater<TrieMap, Object> rootupdater;
    private volatile Object root;

    TrieMap(final Object r, final AtomicReferenceFieldUpdater<TrieMap, Object> rtupd, final Hashing<K> hashf, final Equiv<K> ef) {
        this.r = r;
        this.rtupd = rtupd;
        this.hashf = hashf;
        this.ef = ef;
        this.hashingobj = (hashf instanceof Default) ? hashf : hashf;
        equalityobj = ef;
        rootupdater = rtupd;
        root = r;
    }

    public TrieMap(final Hashing<K> hashf, final Equiv<K> ef) {
        this(INode.newRootNode(), AtomicReferenceFieldUpdater.newUpdater(TrieMap.class, Object.class, "root"), hashf, ef);
    }

    public TrieMap() {
        this(new Default<K>(), Equiv.universal);
    }

    public static <K, V> TrieMap<K, V> empty() {
        return new TrieMap<K, V>();
    }

    Hashing<K> hashing() {
        return hashingobj;
    }

    Equiv<K> equality() {
        return equalityobj;
    }

    final boolean CAS_ROOT(Object ov, Object nv) {
        return rootupdater.compareAndSet(this, ov, nv);
    }

    // FIXME: abort = false by default
    final INode<K, V> readRoot(boolean abort) {
        return RDCSS_READ_ROOT(abort);
    }

    final INode<K, V> readRoot() {
        return RDCSS_READ_ROOT(false);
    }

    final INode<K, V> RDCSS_READ_ROOT() {
        return RDCSS_READ_ROOT(false);
    }

    final INode<K, V> RDCSS_READ_ROOT(boolean abort) {
        Object r = /* READ */root;
        if (r instanceof INode)
            return (INode<K, V>) r;
        else if (r instanceof RDCSS_Descriptor) {
            return RDCSS_Complete(abort);
        }
        throw new RuntimeException("Should not happen");
    }

    private final INode<K, V> RDCSS_Complete(final boolean abort) {
        while (true) {
            Object v = /* READ */root;
            if (v instanceof INode)
                return (INode<K, V>) v;
            else if (v instanceof RDCSS_Descriptor) {
                RDCSS_Descriptor<K, V> desc = (RDCSS_Descriptor<K, V>) v;
                INode<K, V> ov = desc.old;
                MainNode<K, V> exp = desc.expectedmain;
                INode<K, V> nv = desc.nv;

                if (abort) {
                    if (CAS_ROOT(desc, ov))
                        return ov;
                    else {
                        // return RDCSS_Complete (abort);
                        // tailrec
                        continue;
                    }
                }
                else {
                    MainNode<K, V> oldmain = ov.gcasRead(this);
                    if (oldmain == exp) {
                        if (CAS_ROOT(desc, nv)) {
                            desc.committed = true;
                            return nv;
                        }
                        else {
                            // return RDCSS_Complete (abort);
                            // tailrec
                            continue;
                        }
                    }
                    else {
                        if (CAS_ROOT(desc, ov))
                            return ov;
                        else {
                            // return RDCSS_Complete (abort);
                            // tailrec
                            continue;

                        }
                    }
                }
            }

            throw new RuntimeException("Should not happen");
        }
    }

    private boolean RDCSS_ROOT(final INode<K, V> ov, final MainNode<K, V> expectedmain, final INode<K, V> nv) {
        RDCSS_Descriptor<K, V> desc = new RDCSS_Descriptor<K, V>(ov, expectedmain, nv);
        if (CAS_ROOT(ov, desc)) {
            RDCSS_Complete(false);
            return /* READ */desc.committed;
        }
        else
            return false;
    }

    private void inserthc(final K k, final int hc, final V v) {
        while (true) {
            INode<K, V> r = RDCSS_READ_ROOT();
            if (!r.rec_insert(k, v, hc, 0, null, r.gen, this)) {
                // inserthc (k, hc, v);
                // tailrec
                continue;
            }
            break;
        }
    }

    private Option<V> insertifhc(final K k, final int hc, final V v, final Object cond) {
        while (true) {
            INode<K, V> r = RDCSS_READ_ROOT();

            Option<V> ret = r.rec_insertif(k, v, hc, cond, 0, null, r.gen, this);
            if (ret == null) {
                // return insertifhc (k, hc, v, cond);
                // tailrec
                continue;
            }
            else
                return ret;
        }
    }

    private Object lookuphc(final K k, final int hc) {
        while (true) {
            INode<K, V> r = RDCSS_READ_ROOT();
            Object res = r.rec_lookup(k, hc, 0, null, r.gen, this);
            if (res == INodeBase.RESTART) {
                // return lookuphc (k, hc);
                // tailrec
                continue;
            }
            else
                return res;
        }
    }

    /* internal methods */

    // private void writeObject(java.io.ObjectOutputStream out) {
    // out.writeObject(hashf);
    // out.writeObject(ef);
    //
    // Iterator it = iterator();
    // while (it.hasNext) {
    // val (k, v) = it.next();
    // out.writeObject(k);
    // out.writeObject(v);
    // }
    // out.writeObject(TrieMapSerializationEnd);
    // }
    //
    // private TrieMap readObject(java.io.ObjectInputStream in) {
    // root = INode.newRootNode();
    // rootupdater = AtomicReferenceFieldUpdater.newUpdater(TrieMap.class,
    // Object.class, "root");
    //
    // hashingobj = in.readObject();
    // equalityobj = in.readObject();
    //
    // Object obj = null;
    // do {
    // obj = in.readObject();
    // if (obj != TrieMapSerializationEnd) {
    // K k = (K)obj;
    // V = (V)in.readObject();
    // update(k, v);
    // }
    // } while (obj != TrieMapSerializationEnd);
    // }

    private Option<V> removehc(final K k, final V v, final int hc) {
        while (true) {
            INode<K, V> r = RDCSS_READ_ROOT();
            Option<V> res = r.rec_remove(k, v, hc, 0, null, r.gen, this);
            if (res != null)
                return res;
            else {
                // return removehc (k, v, hc);
                // tailrec
                continue;
            }
        }
    }

    public String string() {
        // RDCSS_READ_ROOT().string(0);
        return "Root";
    }

    final boolean isReadOnly() {
        return rootupdater == null;
    }

    final boolean nonReadOnly() {
        return rootupdater != null;
    }

    /**
     * Returns a snapshot of this TrieMap. This operation is lock-free and
     * linearizable.
     * <p/>
     * The snapshot is lazily updated - the first time some branch in the
     * snapshot or this TrieMap are accessed, they are rewritten. This means
     * that the work of rebuilding both the snapshot and this TrieMap is
     * distributed across all the threads doing updates or accesses subsequent
     * to the snapshot creation.
     */

    final public TrieMap<K, V> snapshot() {
        while (true) {
            INode<K, V> r = RDCSS_READ_ROOT();
            final MainNode<K, V> expmain = r.gcasRead(this);
            if (RDCSS_ROOT(r, expmain, r.copyToGen(new Gen(), this)))
                return new TrieMap<K, V>(r.copyToGen(new Gen(), this), rootupdater, hashing(), equality());
            else {
                // return snapshot ();
                // tailrec
                continue;
            }
        }
    }

    /**
     * Returns a read-only snapshot of this TrieMap. This operation is lock-free
     * and linearizable.
     * <p/>
     * The snapshot is lazily updated - the first time some branch of this
     * TrieMap are accessed, it is rewritten. The work of creating the snapshot
     * is thus distributed across subsequent updates and accesses on this
     * TrieMap by all threads. Note that the snapshot itself is never rewritten
     * unlike when calling the `snapshot` method, but the obtained snapshot
     * cannot be modified.
     * <p/>
     * This method is used by other methods such as `size` and `iterator`.
     */
    final public TrieMap<K, V> readOnlySnapshot() {
        // Is it a snapshot of a read-only snapshot?
        if (!nonReadOnly())
            return this;

        while (true) {
            INode<K, V> r = RDCSS_READ_ROOT();
            MainNode<K, V> expmain = r.gcasRead(this);
            if (RDCSS_ROOT(r, expmain, r.copyToGen(new Gen(), this)))
                return new TrieMap<K, V>(r, null, hashing(), equality());
            else {
                // return readOnlySnapshot ();
                continue;
            }
        }
    }

    final public void clear() {
        while (true) {
            INode<K, V> r = RDCSS_READ_ROOT();
            if (!RDCSS_ROOT(r, r.gcasRead(this), INode.<K, V>newRootNode())) {
                continue;
            }
            else {
                return;
            }
        }
    }

    // @inline
    int computeHash(K k) {
        return hashingobj.hash(k);
    }

    final V lookup(K k) {
        int hc = computeHash(k);
//        return (V) lookuphc (k, hc);
        Object o = lookuphc(k, hc);
        if (o instanceof Some) {
            return ((Some<V>) o).get();
        }
        else if (o instanceof None)
            return null;
        else
            return (V) o;
    }

    final V apply(K k) {
        int hc = computeHash(k);
        Object res = lookuphc(k, hc);
        if (res == null)
            throw new NoSuchElementException();
        else
            return (V) res;
    }

    final public V get(Object k) {
        return lookup((K) k);
    }

    final public Option<V> putOpt(Object key, Object value) {
        int hc = computeHash((K) key);
        return insertifhc((K) key, hc, (V) value, null);
    }

    /* public methods */

    // public Seq<V> seq() {
    // return this;
    // }

    // override def par = new ParTrieMap(this)

    // static TrieMap empty() {
    // return new TrieMap();
    // }

    final public V put(Object key, Object value) {
        int hc = computeHash((K) key);
        Option<V> ov = insertifhc((K) key, hc, (V) value, null);
        if (ov instanceof Some) {
            Some<V> sv = (Some<V>) ov;
            return sv.get();
        }
        else
            return null;
    }

    final public void update(K k, V v) {
        int hc = computeHash(k);
        inserthc(k, hc, v);
    }

    final public TrieMap<K, V> add(K k, V v) {
        update(k, v);
        return this;
    }

    final Option<V> removeOpt(K k) {
        int hc = computeHash(k);
        return removehc(k, (V) null, hc);
    }

    final public V remove(Object k) {
        int hc = computeHash((K) k);
        Option<V> ov = removehc((K) k, (V) null, hc);
        if (ov instanceof Some) {
            Some<V> sv = (Some<V>) ov;
            return sv.get();
        }
        else
            return null;
    }

    final public Option<V> putIfAbsentOpt(K k, V v) {
        int hc = computeHash(k);
        return insertifhc(k, hc, v, INode.KEY_ABSENT);
    }

    final public V putIfAbsent(Object k, Object v) {
        int hc = computeHash((K) k);
        Option<V> ov = insertifhc((K) k, hc, (V) v, INode.KEY_ABSENT);
        if (ov instanceof Some) {
            Some<V> sv = (Some<V>) ov;
            return sv.get();
        }
        else
            return null;
    }

    public boolean remove(Object k, Object v) {
        int hc = computeHash((K) k);
        return removehc((K) k, (V) v, hc).nonEmpty();
    }

//    final public Option<V> get (K k) {
//        int hc = computeHash (k);
//        return Option.makeOption ((V)lookuphc (k, hc));
//    }

    public boolean replace(K k, V oldvalue, V newvalue) {
        int hc = computeHash(k);
        return insertifhc(k, hc, newvalue, (Object) oldvalue).nonEmpty();
    }

    public Option<V> replaceOpt(K k, V v) {
        int hc = computeHash(k);
        return insertifhc(k, hc, v, INode.KEY_PRESENT);
    }

    public V replace(Object k, Object v) {
        int hc = computeHash((K) k);
        Option<V> ov = insertifhc((K) k, hc, (V) v, INode.KEY_PRESENT);
        if (ov instanceof Some) {
            Some<V> sv = (Some<V>) ov;
            return sv.get();
        }
        else
            return null;
    }

    /**
     * Return an iterator over a TrieMap.
     * <p/>
     * If this is a read-only snapshot, it would return a read-only iterator.
     * <p/>
     * If it is the original TrieMap or a non-readonly snapshot, it would return
     * an iterator that would allow for updates.
     *
     * @return
     */
    public Iterator<Entry<K, V>> iterator() {
        if (!nonReadOnly())
            return readOnlySnapshot().readOnlyIterator();
        else
            return new TrieMapIterator<K, V>(0, this);
    }

    /**
     * Return an iterator over a TrieMap.
     * This is a read-only iterator.
     *
     * @return
     */
    public Iterator<Entry<K, V>> readOnlyIterator() {
        if (nonReadOnly())
            return readOnlySnapshot().readOnlyIterator();
        else
            return new TrieMapReadOnlyIterator<K, V>(0, this);
    }

    private int cachedSize() {
        INode<K, V> r = RDCSS_READ_ROOT();
        return r.cachedSize(this);
    }

    public int size() {
        if (nonReadOnly())
            return readOnlySnapshot().size();
        else
            return cachedSize();
    }

//    final public TrieMap<K, V> remove (Object k) {
//        removeOpt ((K)k);
//        return this;
//    }

    String stringPrefix() {
        return "TrieMap";
    }

    public boolean containsKey(Object key) {
        return lookup((K) key) != null;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return entrySet;
    }

    private interface KVNode<K, V> {
        Entry<K, V> kvPair();
    }

    private static interface Hashing<K> {
        public int hash(K k);
    }

    static class INode<K, V> extends INodeBase<K, V> {
        static Object KEY_PRESENT = new Object();
        static Object KEY_ABSENT = new Object();

        public INode(MainNode<K, V> bn, Gen g) {
            super(g);
            WRITE(bn);
        }

        public INode(Gen g) {
            this(null, g);
        }

        static <K, V> INode<K, V> newRootNode() {
            Gen gen = new Gen();
            CNode<K, V> cn = new CNode<K, V>(0, new BasicNode[]{}, gen);
            return new INode<K, V>(cn, gen);
        }

        final void WRITE(final MainNode<K, V> nval) {
            INodeBase.updater.set(this, nval);
        }

        final boolean CAS(final MainNode<K, V> old, final MainNode<K, V> n) {
            return INodeBase.updater.compareAndSet(this, old, n);
        }

        final MainNode<K, V> gcasRead(final TrieMap<K, V> ct) {
            return GCAS_READ(ct);
        }

        final MainNode<K, V> GCAS_READ(TrieMap<K, V> ct) {
            MainNode<K, V> m = /* READ */mainnode;
            MainNode<K, V> prevval = /* READ */m.prev;
            if (prevval == null)
                return m;
            else
                return GCAS_Complete(m, ct);
        }

        private MainNode<K, V> GCAS_Complete(MainNode<K, V> m, final TrieMap<K, V> ct) {
            while (true) {
                if (m == null)
                    return null;
                else {
                    // complete the GCAS
                    MainNode<K, V> prev = /* READ */m.prev;
                    INode<K, V> ctr = ct.readRoot(true);

                    if (prev == null)
                        return m;

                    if (prev instanceof FailedNode) {
                        // try to commit to previous value
                        FailedNode<K, V> fn = (FailedNode<K, V>) prev;
                        if (CAS(m, fn.prev))
                            return fn.prev;
                        else {
                            // Tailrec
                            // return GCAS_Complete (/* READ */mainnode, ct);
                            m = /* READ */mainnode;
                            continue;
                        }
                    }
                    else if (prev instanceof MainNode) {
                        // Assume that you've read the root from the generation
                        // G.
                        // Assume that the snapshot algorithm is correct.
                        // ==> you can only reach nodes in generations <= G.
                        // ==> `gen` is <= G.
                        // We know that `ctr.gen` is >= G.
                        // ==> if `ctr.gen` = `gen` then they are both equal to
                        // G.
                        // ==> otherwise, we know that either `ctr.gen` > G,
                        // `gen` <
                        // G,
                        // or both
                        if ((ctr.gen == gen) && ct.nonReadOnly()) {
                            // try to commit
                            if (m.CAS_PREV(prev, null))
                                return m;
                            else {
                                // return GCAS_Complete (m, ct);
                                // tailrec
                                continue;
                            }
                        }
                        else {
                            // try to abort
                            m.CAS_PREV(prev, new FailedNode<K, V>(prev));
                            return GCAS_Complete(/* READ */mainnode, ct);
                        }
                    }
                }
                throw new RuntimeException("Should not happen");
            }
        }

        final boolean GCAS(final MainNode<K, V> old, final MainNode<K, V> n, final TrieMap<K, V> ct) {
            n.WRITE_PREV(old);
            if (CAS(old, n)) {
                GCAS_Complete(n, ct);
                return /* READ */n.prev == null;
            }
            else
                return false;
        }

        private boolean equal(final K k1, final K k2, final TrieMap<K, V> ct) {
            return ct.equality().equiv(k1, k2);
        }

        private INode<K, V> inode(final MainNode<K, V> cn) {
            INode<K, V> nin = new INode<K, V>(gen);
            nin.WRITE(cn);
            return nin;
        }

        final INode<K, V> copyToGen(final Gen ngen, final TrieMap<K, V> ct) {
            INode<K, V> nin = new INode<K, V>(ngen);
            MainNode<K, V> main = GCAS_READ(ct);
            nin.WRITE(main);
            return nin;
        }

        /**
         * Inserts a key value pair, overwriting the old pair if the keys match.
         *
         * @return true if successful, false otherwise
         */
        final boolean rec_insert(final K k, final V v, final int hc, final int lev, final INode<K, V> parent, final Gen startgen, final TrieMap<K, V> ct) {
            while (true) {
                MainNode<K, V> m = GCAS_READ(ct); // use -Yinline!

                if (m instanceof CNode) {
                    // 1) a multiway node
                    CNode<K, V> cn = (CNode<K, V>) m;
                    int idx = (hc >>> lev) & 0x1f;
                    int flag = 1 << idx;
                    int bmp = cn.bitmap;
                    int mask = flag - 1;
                    int pos = Integer.bitCount(bmp & mask);
                    if ((bmp & flag) != 0) {
                        // 1a) insert below
                        BasicNode cnAtPos = cn.array[pos];
                        if (cnAtPos instanceof INode) {
                            INode<K, V> in = (INode<K, V>) cnAtPos;
                            if (startgen == in.gen)
                                return in.rec_insert(k, v, hc, lev + 5, this, startgen, ct);
                            else {
                                if (GCAS(cn, cn.renewed(startgen, ct), ct)) {
                                    // return rec_insert (k, v, hc, lev, parent,
                                    // startgen, ct);
                                    // tailrec
                                    continue;
                                }
                                else
                                    return false;
                            }
                        }
                        else if (cnAtPos instanceof SNode) {
                            SNode<K, V> sn = (SNode<K, V>) cnAtPos;
                            if (sn.hc == hc && equal((K) sn.k, k, ct))
                                return GCAS(cn, cn.updatedAt(pos, new SNode<K, V>(k, v, hc), gen), ct);
                            else {
                                CNode<K, V> rn = (cn.gen == gen) ? cn : cn.renewed(gen, ct);
                                MainNode<K, V> nn = rn.updatedAt(pos, inode(CNode.dual(sn, sn.hc, new SNode(k, v, hc), hc, lev + 5, gen)), gen);
                                return GCAS(cn, nn, ct);
                            }
                        }
                    }
                    else {
                        CNode<K, V> rn = (cn.gen == gen) ? cn : cn.renewed(gen, ct);
                        MainNode<K, V> ncnode = rn.insertedAt(pos, flag, new SNode<K, V>(k, v, hc), gen);
                        return GCAS(cn, ncnode, ct);
                    }
                }
                else if (m instanceof TNode) {
                    clean(parent, ct, lev - 5);
                    return false;
                }
                else if (m instanceof LNode) {
                    LNode<K, V> ln = (LNode<K, V>) m;
                    MainNode<K, V> nn = ln.inserted(k, v);
                    return GCAS(ln, nn, ct);
                }

                throw new RuntimeException("Should not happen");
            }
        }

        /**
         * Inserts a new key value pair, given that a specific condition is met.
         *
         * @param cond null - don't care if the key was there
         *             KEY_ABSENT - key wasn't there
         *             KEY_PRESENT - key was there
         *             other value `v` - key must be bound to `v`
         * @return null if unsuccessful, Option[V] otherwise (indicating
         * previous value bound to the key)
         */
        final Option<V> rec_insertif(final K k, final V v, final int hc, final Object cond, final int lev, final INode<K, V> parent, final Gen startgen, final TrieMap<K, V> ct) {
            while (true) {
                MainNode<K, V> m = GCAS_READ(ct); // use -Yinline!

                if (m instanceof CNode) {
                    // 1) a multiway node
                    CNode<K, V> cn = (CNode<K, V>) m;
                    int idx = (hc >>> lev) & 0x1f;
                    int flag = 1 << idx;
                    int bmp = cn.bitmap;
                    int mask = flag - 1;
                    int pos = Integer.bitCount(bmp & mask);

                    if ((bmp & flag) != 0) {
                        // 1a) insert below
                        BasicNode cnAtPos = cn.array[pos];
                        if (cnAtPos instanceof INode) {
                            INode<K, V> in = (INode<K, V>) cnAtPos;
                            if (startgen == in.gen)
                                return in.rec_insertif(k, v, hc, cond, lev + 5, this, startgen, ct);
                            else {
                                if (GCAS(cn, cn.renewed(startgen, ct), ct)) {
                                    // return rec_insertif (k, v, hc, cond, lev,
                                    // parent, startgen, ct);
                                    // tailrec
                                    continue;
                                }
                                else
                                    return null;
                            }
                        }
                        else if (cnAtPos instanceof SNode) {
                            SNode<K, V> sn = (SNode<K, V>) cnAtPos;
                            if (cond == null) {
                                if (sn.hc == hc && equal(sn.k, k, ct)) {
                                    if (GCAS(cn, cn.updatedAt(pos, new SNode<K, V>(k, v, hc), gen), ct))
                                        return Option.makeOption(sn.v);
                                    else
                                        return null;
                                }
                                else {
                                    CNode<K, V> rn = (cn.gen == gen) ? cn : cn.renewed(gen, ct);
                                    MainNode<K, V> nn = rn.updatedAt(pos, inode(CNode.dual(sn, sn.hc, new SNode(k, v, hc), hc, lev + 5, gen)), gen);
                                    if (GCAS(cn, nn, ct))
                                        return Option.makeOption(); // None;
                                    else
                                        return null;
                                }

                            }
                            else if (cond == INode.KEY_ABSENT) {
                                if (sn.hc == hc && equal(sn.k, k, ct))
                                    return Option.makeOption(sn.v);
                                else {
                                    CNode<K, V> rn = (cn.gen == gen) ? cn : cn.renewed(gen, ct);
                                    MainNode<K, V> nn = rn.updatedAt(pos, inode(CNode.dual(sn, sn.hc, new SNode(k, v, hc), hc, lev + 5, gen)), gen);
                                    if (GCAS(cn, nn, ct))
                                        return Option.makeOption(); // None
                                    else
                                        return null;
                                }
                            }
                            else if (cond == INode.KEY_PRESENT) {
                                if (sn.hc == hc && equal(sn.k, k, ct)) {
                                    if (GCAS(cn, cn.updatedAt(pos, new SNode<K, V>(k, v, hc), gen), ct))
                                        return Option.makeOption(sn.v);
                                    else
                                        return null;

                                }
                                else
                                    return Option.makeOption();// None;
                            }
                            else {
                                if (sn.hc == hc && equal(sn.k, k, ct) && sn.v == cond) {
                                    if (GCAS(cn, cn.updatedAt(pos, new SNode<K, V>(k, v, hc), gen), ct))
                                        return Option.makeOption(sn.v);
                                    else
                                        return null;
                                }
                                else
                                    return Option.makeOption(); // None
                            }

                        }
                    }
                    else if (cond == null || cond == INode.KEY_ABSENT) {
                        CNode<K, V> rn = (cn.gen == gen) ? cn : cn.renewed(gen, ct);
                        CNode<K, V> ncnode = rn.insertedAt(pos, flag, new SNode<K, V>(k, v, hc), gen);
                        if (GCAS(cn, ncnode, ct))
                            return Option.makeOption();// None
                        else
                            return null;
                    }
                    else if (cond == INode.KEY_PRESENT) {
                        return Option.makeOption();// None;
                    }
                    else
                        return Option.makeOption(); // None
                }
                else if (m instanceof TNode) {
                    clean(parent, ct, lev - 5);
                    return null;
                }
                else if (m instanceof LNode) {
                    // 3) an l-node
                    LNode<K, V> ln = (LNode<K, V>) m;
                    if (cond == null) {
                        Option<V> optv = ln.get(k);
                        if (insertln(ln, k, v, ct))
                            return optv;
                        else
                            return null;
                    }
                    else if (cond == INode.KEY_ABSENT) {
                        Option<V> t = ln.get(k);
                        if (t == null) {
                            if (insertln(ln, k, v, ct))
                                return Option.makeOption();// None
                            else
                                return null;
                        }
                        else
                            return t;
                    }
                    else if (cond == INode.KEY_PRESENT) {
                        Option<V> t = ln.get(k);
                        if (t != null) {
                            if (insertln(ln, k, v, ct))
                                return t;
                            else
                                return null;
                        }
                        else
                            return null; // None
                    }
                    else {
                        Option<V> t = ln.get(k);
                        if (t != null) {
                            if (((Some<V>) t).get() == cond) {
                                if (insertln(ln, k, v, ct))
                                    return new Some<V>((V) cond);
                                else
                                    return null;

                            }
                            else
                                return Option.makeOption();
                        }
                    }
                }

//                throw new RuntimeException ("Should not happen");
            }
        }

        final boolean insertln(final LNode<K, V> ln, final K k, final V v, final TrieMap<K, V> ct) {
            LNode<K, V> nn = ln.inserted(k, v);
            return GCAS(ln, nn, ct);
        }

        /**
         * Looks up the value associated with the key.
         *
         * @return null if no value has been found, RESTART if the operation
         * wasn't successful, or any other value otherwise
         */
        final Object rec_lookup(final K k, final int hc, int lev, INode<K, V> parent, final Gen startgen, final TrieMap<K, V> ct) {
            while (true) {
                MainNode<K, V> m = GCAS_READ(ct); // use -Yinline!

                if (m instanceof CNode) {
                    // 1) a multinode
                    final CNode<K, V> cn = (CNode<K, V>) m;
                    int idx = (hc >>> lev) & 0x1f;
                    int flag = 1 << idx;
                    int bmp = cn.bitmap;
                    if ((bmp & flag) == 0)
                        return null; // 1a) bitmap shows no binding
                    else { // 1b) bitmap contains a value - descend
                        int pos = (bmp == 0xffffffff) ? idx : Integer.bitCount(bmp & (flag - 1));
                        final BasicNode sub = cn.array[pos];
                        if (sub instanceof INode) {
                            INode<K, V> in = (INode<K, V>) sub;
                            if (ct.isReadOnly() || (startgen == ((INodeBase<K, V>) sub).gen))
                                return in.rec_lookup(k, hc, lev + 5, this, startgen, ct);
                            else {
                                if (GCAS(cn, cn.renewed(startgen, ct), ct)) {
                                    // return rec_lookup (k, hc, lev, parent,
                                    // startgen, ct);
                                    // Tailrec
                                    continue;
                                }
                                else
                                    return RESTART; // used to be throw
                                // RestartException
                            }
                        }
                        else if (sub instanceof SNode) {
                            // 2) singleton node
                            SNode<K, V> sn = (SNode<K, V>) sub;
                            if (((SNode) sub).hc == hc && equal(sn.k, k, ct))
                                return sn.v;
                            else
                                return null;
                        }
                    }
                }
                else if (m instanceof TNode) {
                    // 3) non-live node
                    return cleanReadOnly((TNode<K, V>) m, lev, parent, ct, k, hc);
                }
                else if (m instanceof LNode) {
                    // 5) an l-node
                    Option<V> tmp = ((LNode<K, V>) m).get(k);
                    return (tmp instanceof Option) ? ((Option<V>) tmp) : null;
                }

                throw new RuntimeException("Should not happen");
            }
        }

        private Object cleanReadOnly(final TNode<K, V> tn, final int lev, final INode<K, V> parent, final TrieMap<K, V> ct, K k, int hc) {
            if (ct.nonReadOnly()) {
                clean(parent, ct, lev - 5);
                return RESTART; // used to be throw RestartException
            }
            else {
                if (tn.hc == hc && tn.k == k)
                    return tn.v;
                else
                    return null;
            }
        }

        /**
         * Removes the key associated with the given value.
         *
         * @param v if null, will remove the key irregardless of the value;
         *          otherwise removes only if binding contains that exact key
         *          and value
         * @return null if not successful, an Option[V] indicating the previous
         * value otherwise
         */
        final Option<V> rec_remove(K k, V v, int hc, int lev, final INode<K, V> parent, final Gen startgen, final TrieMap<K, V> ct) {
            MainNode<K, V> m = GCAS_READ(ct); // use -Yinline!

            if (m instanceof CNode) {
                CNode<K, V> cn = (CNode<K, V>) m;
                int idx = (hc >>> lev) & 0x1f;
                int bmp = cn.bitmap;
                int flag = 1 << idx;
                if ((bmp & flag) == 0)
                    return Option.makeOption();
                else {
                    int pos = Integer.bitCount(bmp & (flag - 1));
                    BasicNode sub = cn.array[pos];
                    Option<V> res = null;
                    if (sub instanceof INode) {
                        INode<K, V> in = (INode<K, V>) sub;
                        if (startgen == in.gen)
                            res = in.rec_remove(k, v, hc, lev + 5, this, startgen, ct);
                        else {
                            if (GCAS(cn, cn.renewed(startgen, ct), ct))
                                res = rec_remove(k, v, hc, lev, parent, startgen, ct);
                            else
                                res = null;
                        }

                    }
                    else if (sub instanceof SNode) {
                        SNode<K, V> sn = (SNode<K, V>) sub;
                        if (sn.hc == hc && equal(sn.k, k, ct) && (v == null || v.equals(sn.v))) {
                            MainNode<K, V> ncn = cn.removedAt(pos, flag, gen).toContracted(lev);
                            if (GCAS(cn, ncn, ct))
                                res = Option.makeOption(sn.v);
                            else
                                res = null;
                        }
                        else
                            res = Option.makeOption();
                    }

                    if (res instanceof None || (res == null))
                        return res;
                    else {
                        if (parent != null) { // never tomb at root
                            MainNode<K, V> n = GCAS_READ(ct);
                            if (n instanceof TNode)
                                cleanParent(n, parent, ct, hc, lev, startgen);
                        }

                        return res;
                    }
                }
            }
            else if (m instanceof TNode) {
                clean(parent, ct, lev - 5);
                return null;
            }
            else if (m instanceof LNode) {
                LNode<K, V> ln = (LNode<K, V>) m;
                if (v == null) {
                    Option<V> optv = ln.get(k);
                    MainNode<K, V> nn = ln.removed(k, ct);
                    if (GCAS(ln, nn, ct))
                        return optv;
                    else
                        return null;
                }
                else {
                    Option<V> tmp = ln.get(k);
                    if (tmp instanceof Some) {
                        Some<V> tmp1 = (Some<V>) tmp;
                        if (tmp1.get() == v) {
                            MainNode<K, V> nn = ln.removed(k, ct);
                            if (GCAS(ln, nn, ct))
                                return tmp;
                            else
                                return null;
                        }
                    }
                }
            }
            throw new RuntimeException("Should not happen");
        }

        void cleanParent(final Object nonlive, final INode<K, V> parent, final TrieMap<K, V> ct, final int hc, final int lev, final Gen startgen) {
            while (true) {
                MainNode<K, V> pm = parent.GCAS_READ(ct);
                if (pm instanceof CNode) {
                    CNode<K, V> cn = (CNode<K, V>) pm;
                    int idx = (hc >>> (lev - 5)) & 0x1f;
                    int bmp = cn.bitmap;
                    int flag = 1 << idx;
                    if ((bmp & flag) == 0) {
                    } // somebody already removed this i-node, we're done
                    else {
                        int pos = Integer.bitCount(bmp & (flag - 1));
                        BasicNode sub = cn.array[pos];
                        if (sub == this) {
                            if (nonlive instanceof TNode) {
                                TNode<K, V> tn = (TNode<K, V>) nonlive;
                                MainNode<K, V> ncn = cn.updatedAt(pos, tn.copyUntombed(), gen).toContracted(lev - 5);
                                if (!parent.GCAS(cn, ncn, ct))
                                    if (ct.readRoot().gen == startgen) {
                                        // cleanParent (nonlive, parent, ct, hc,
                                        // lev, startgen);
                                        // tailrec
                                        continue;
                                    }
                            }
                        }
                    }
                }
                else {
                    // parent is no longer a cnode, we're done
                }
                break;
            }
        }

        private void clean(final INode<K, V> nd, final TrieMap<K, V> ct, int lev) {
            MainNode<K, V> m = nd.GCAS_READ(ct);
            if (m instanceof CNode) {
                CNode<K, V> cn = (CNode<K, V>) m;
                nd.GCAS(cn, cn.toCompressed(ct, lev, gen), ct);
            }
        }

        final boolean isNullInode(final TrieMap<K, V> ct) {
            return GCAS_READ(ct) == null;
        }

        final int cachedSize(final TrieMap<K, V> ct) {
            MainNode<K, V> m = GCAS_READ(ct);
            return m.cachedSize(ct);
        }

        // /* this is a quiescent method! */
        // def string(lev: Int) = "%sINode -> %s".format("  " * lev, mainnode
        // match {
        // case null => "<null>"
        // case tn: TNode[_, _] => "TNode(%s, %s, %d, !)".format(tn.k, tn.v,
        // tn.hc)
        // case cn: CNode[_, _] => cn.string(lev)
        // case ln: LNode[_, _] => ln.string(lev)
        // case x => "<elem: %s>".format(x)
        // })

        public String string(int lev) {
            return "INode";
        }

    }

    private static final class FailedNode<K, V> extends MainNode<K, V> {
        MainNode<K, V> p;

        FailedNode(final MainNode<K, V> p) {
            this.p = p;
            WRITE_PREV(p);
        }

        public String string(int lev) {
            throw new UnsupportedOperationException();
        }

        public int cachedSize(Object ct) {
            throw new UnsupportedOperationException();
        }

        public String toString() {
            return String.format("FailedNode(%s)", p);
        }
    }

    private static final class SNode<K, V> extends BasicNode implements KVNode<K, V> {
        final K k;
        final V v;
        final int hc;

        SNode(final K k, final V v, final int hc) {
            this.k = k;
            this.v = v;
            this.hc = hc;
        }

        final SNode<K, V> copy() {
            return new SNode<K, V>(k, v, hc);
        }

        final TNode<K, V> copyTombed() {
            return new TNode<K, V>(k, v, hc);
        }

        final SNode<K, V> copyUntombed() {
            return new SNode<K, V>(k, v, hc);
        }

        final public Entry<K, V> kvPair() {
            return new Pair<K, V>(k, v);
        }

        final public String string(int lev) {
            // ("  " * lev) + "SNode(%s, %s, %x)".format(k, v, hc);
            return "SNode";
        }
    }

    private static final class TNode<K, V> extends MainNode<K, V> implements KVNode<K, V> {
        final K k;
        final V v;
        final int hc;

        TNode(final K k, final V v, final int hc) {
            this.k = k;
            this.v = v;
            this.hc = hc;
        }

        final TNode<K, V> copy() {
            return new TNode<K, V>(k, v, hc);
        }

        final TNode<K, V> copyTombed() {
            return new TNode<K, V>(k, v, hc);
        }

        final SNode<K, V> copyUntombed() {
            return new SNode<K, V>(k, v, hc);
        }

        final public Pair<K, V> kvPair() {
            return new Pair<K, V>(k, v);
        }

        final public int cachedSize(Object ct) {
            return 1;
        }

        final public String string(int lev) {
            // ("  " * lev) + "TNode(%s, %s, %x, !)".format(k, v, hc);
            return "TNode";
        }
    }

    private final static class LNode<K, V> extends MainNode<K, V> {
        final ListMap<K, V> listmap;

        public LNode(final ListMap<K, V> listmap) {
            this.listmap = listmap;
        }

        public LNode(K k, V v) {
            this(ListMap.map(k, v));
        }

        public LNode(K k1, V v1, K k2, V v2) {
            this(ListMap.map(k1, v1, k2, v2));
        }

        LNode<K, V> inserted(K k, V v) {
            return new LNode<K, V>(listmap.add(k, v));
        }

        MainNode<K, V> removed(K k, final TrieMap<K, V> ct) {
            ListMap<K, V> updmap = listmap.remove(k);
            if (updmap.size() > 1)
                return new LNode<K, V>(updmap);
            else {
                Entry<K, V> kv = updmap.iterator().next();
                // create it tombed so that it gets compressed on subsequent
                // accesses
                return new TNode<K, V>(kv.getKey(), kv.getValue(), ct.computeHash(kv.getKey()));
            }
        }

        Option<V> get(K k) {
            return listmap.get(k);
        }

        public int cachedSize(Object ct) {
            return listmap.size();
        }

        public String string(int lev) {
            // (" " * lev) + "LNode(%s)".format(listmap.mkString(", "))
            return "LNode";
        }
    }

    private static final class CNode<K, V> extends CNodeBase<K, V> {

        final int bitmap;
        final BasicNode[] array;
        final Gen gen;

        CNode(final int bitmap, final BasicNode[] array, final Gen gen) {
            this.bitmap = bitmap;
            this.array = array;
            this.gen = gen;
        }

        static <K, V> MainNode<K, V> dual(final SNode<K, V> x, int xhc, final SNode<K, V> y, int yhc, int lev, Gen gen) {
            if (lev < 35) {
                int xidx = (xhc >>> lev) & 0x1f;
                int yidx = (yhc >>> lev) & 0x1f;
                int bmp = (1 << xidx) | (1 << yidx);

                if (xidx == yidx) {
                    INode<K, V> subinode = new INode<K, V>(gen);// (TrieMap.inodeupdater)
                    subinode.mainnode = dual(x, xhc, y, yhc, lev + 5, gen);
                    return new CNode<K, V>(bmp, new BasicNode[]{subinode}, gen);
                }
                else {
                    if (xidx < yidx)
                        return new CNode<K, V>(bmp, new BasicNode[]{x, y}, gen);
                    else
                        return new CNode<K, V>(bmp, new BasicNode[]{y, x}, gen);
                }
            }
            else {
                return new LNode<K, V>(x.k, x.v, y.k, y.v);
            }
        }

        // this should only be called from within read-only snapshots
        final public int cachedSize(Object ct) {
            int currsz = READ_SIZE();
            if (currsz != -1)
                return currsz;
            else {
                int sz = computeSize((TrieMap<K, V>) ct);
                while (READ_SIZE() == -1)
                    CAS_SIZE(-1, sz);
                return READ_SIZE();
            }
        }

        // lends itself towards being parallelizable by choosing
        // a random starting offset in the array
        // => if there are concurrent size computations, they start
        // at different positions, so they are more likely to
        // to be independent
        private int computeSize(final TrieMap<K, V> ct) {
            int i = 0;
            int sz = 0;
            // final int offset = (array.length > 0) ?
            // // util.Random.nextInt(array.length) /* <-- benchmarks show that
            // // this causes observable contention */
            // scala.concurrent.forkjoin.ThreadLocalRandom.current.nextInt (0,
            // array.length)
            // : 0;

            final int offset = 0;
            while (i < array.length) {
                int pos = (i + offset) % array.length;
                BasicNode elem = array[pos];
                if (elem instanceof SNode)
                    sz += 1;
                else if (elem instanceof INode)
                    sz += ((INode<K, V>) elem).cachedSize(ct);
                i += 1;
            }
            return sz;
        }

        final CNode<K, V> updatedAt(int pos, final BasicNode nn, final Gen gen) {
            int len = array.length;
            BasicNode[] narr = new BasicNode[len];
            System.arraycopy(array, 0, narr, 0, len);
            narr[pos] = nn;
            return new CNode<K, V>(bitmap, narr, gen);
        }

        final CNode<K, V> removedAt(int pos, int flag, final Gen gen) {
            BasicNode[] arr = array;
            int len = arr.length;
            BasicNode[] narr = new BasicNode[len - 1];
            System.arraycopy(arr, 0, narr, 0, pos);
            System.arraycopy(arr, pos + 1, narr, pos, len - pos - 1);
            return new CNode<K, V>(bitmap ^ flag, narr, gen);
        }

        final CNode<K, V> insertedAt(int pos, int flag, final BasicNode nn, final Gen gen) {
            int len = array.length;
            int bmp = bitmap;
            BasicNode[] narr = new BasicNode[len + 1];
            System.arraycopy(array, 0, narr, 0, pos);
            narr[pos] = nn;
            System.arraycopy(array, pos, narr, pos + 1, len - pos);
            return new CNode<K, V>(bmp | flag, narr, gen);
        }

        /**
         * Returns a copy of this cnode such that all the i-nodes below it are
         * copied to the specified generation `ngen`.
         */
        final CNode<K, V> renewed(final Gen ngen, final TrieMap<K, V> ct) {
            int i = 0;
            BasicNode[] arr = array;
            int len = arr.length;
            BasicNode[] narr = new BasicNode[len];
            while (i < len) {
                BasicNode elem = arr[i];
                if (elem instanceof INode) {
                    INode<K, V> in = (INode<K, V>) elem;
                    narr[i] = in.copyToGen(ngen, ct);
                }
                else if (elem instanceof BasicNode)
                    narr[i] = elem;
                i += 1;
            }
            return new CNode<K, V>(bitmap, narr, ngen);
        }

        private BasicNode resurrect(final INode<K, V> inode, final Object inodemain) {
            if (inodemain instanceof TNode) {
                TNode<K, V> tn = (TNode<K, V>) inodemain;
                return tn.copyUntombed();
            }
            else
                return inode;
        }

        final MainNode<K, V> toContracted(int lev) {
            if (array.length == 1 && lev > 0) {
                if (array[0] instanceof SNode) {
                    SNode<K, V> sn = (SNode<K, V>) array[0];
                    return sn.copyTombed();
                }
                else
                    return this;

            }
            else
                return this;
        }

        // - if the branching factor is 1 for this CNode, and the child
        // is a tombed SNode, returns its tombed version
        // - otherwise, if there is at least one non-null node below,
        // returns the version of this node with at least some null-inodes
        // removed (those existing when the op began)
        // - if there are only null-i-nodes below, returns null
        final MainNode<K, V> toCompressed(final TrieMap<K, V> ct, int lev, Gen gen) {
            int bmp = bitmap;
            int i = 0;
            BasicNode[] arr = array;
            BasicNode[] tmparray = new BasicNode[arr.length];
            while (i < arr.length) { // construct new bitmap
                BasicNode sub = arr[i];
                if (sub instanceof INode) {
                    INode<K, V> in = (INode<K, V>) sub;
                    MainNode<K, V> inodemain = in.gcasRead(ct);
                    assert (inodemain != null);
                    tmparray[i] = resurrect(in, inodemain);
                }
                else if (sub instanceof SNode) {
                    tmparray[i] = sub;
                }
                i += 1;
            }

            return new CNode<K, V>(bmp, tmparray, gen).toContracted(lev);
        }

        /*
         * quiescently consistent - don't call concurrently to anything
         * involving a GCAS!!
         */
        // protected Seq<K,V> collectElems() {
        // array flatMap {
        // case sn: SNode[K, V] => Some(sn.kvPair)
        // case in: INode[K, V] => in.mainnode match {
        // case tn: TNode[K, V] => Some(tn.kvPair)
        // case ln: LNode[K, V] => ln.listmap.toList
        // case cn: CNode[K, V] => cn.collectElems
        // }
        // }
        // }

        // protected Seq<String> collectLocalElems() {
        // // array flatMap {
        // // case sn: SNode[K, V] => Some(sn.kvPair._2.toString)
        // // case in: INode[K, V] => Some(in.toString.drop(14) + "(" + in.gen +
        // ")")
        // // }
        // return null;
        // }

        public String string(int lev) {
            // "CNode %x\n%s".format(bitmap, array.map(_.string(lev +
            // 1)).mkString("\n"));
            return "CNode";
        }

        public String toString() {
            // val elems = collectLocalElems
            // "CNode(sz: %d; %s)".format(elems.size,
            // elems.sorted.mkString(", "))
            return "CNode";
        }

    }

    private static class RDCSS_Descriptor<K, V> {
        INode<K, V> old;
        MainNode<K, V> expectedmain;
        INode<K, V> nv;
        volatile boolean committed = false;

        public RDCSS_Descriptor(final INode<K, V> old, final MainNode<K, V> expectedmain, final INode<K, V> nv) {
            this.old = old;
            this.expectedmain = expectedmain;
            this.nv = nv;
        }

    }

    private static class Equiv<K> {
        static Equiv universal = new Equiv();

        public boolean equiv(K k1, K k2) {
            return k1.equals(k2);
        }
    }

    static class Default<K> implements Hashing<K> {
        public int hash(K k) {
            int h = k.hashCode();
            // This function ensures that hashCodes that differ only by
            // constant multiples at each bit position have a bounded
            // number of collisions (approximately 8 at default load factor).
            h ^= (h >>> 20) ^ (h >>> 12);
            h ^= (h >>> 7) ^ (h >>> 4);
            return h;
        }
    }

    /**
     * This iterator is a read-only one and does not allow for any update
     * operations on the underlying data structure.
     *
     * @param <K>
     * @param <V>
     */
    private static class TrieMapReadOnlyIterator<K, V> extends TrieMapIterator<K, V> {
        TrieMapReadOnlyIterator(int level, final TrieMap<K, V> ct, boolean mustInit) {
            super(level, ct, mustInit);
        }

        TrieMapReadOnlyIterator(int level, TrieMap<K, V> ct) {
            this(level, ct, true);
        }

        void initialize() {
            assert (ct.isReadOnly());
            super.initialize();
        }

        public void remove() {
            throw new RuntimeException("Operation not supported for read-only iterators");
        }


        Entry<K, V> nextEntry(final Entry<K, V> rr) {
            // Return non-updatable entry
            return rr;
        }
    }

    private static class TrieMapIterator<K, V> implements Iterator<Entry<K, V>> {
        private final boolean mustInit;
        protected TrieMap<K, V> ct;
        private int level;
        private BasicNode[][] stack = new BasicNode[7][];
        private int[] stackpos = new int[7];
        private int depth = -1;
        private Iterator<Entry<K, V>> subiter = null;
        private KVNode<K, V> current = null;
        private Entry<K, V> lastReturned = null;

        TrieMapIterator(int level, final TrieMap<K, V> ct, boolean mustInit) {
            this.level = level;
            this.ct = ct;
            this.mustInit = mustInit;
            if (this.mustInit)
                initialize();
        }

        TrieMapIterator(int level, TrieMap<K, V> ct) {
            this(level, ct, true);
        }


        public boolean hasNext() {
            return (current != null) || (subiter != null);
        }

        public Entry<K, V> next() {
            if (hasNext()) {
                Entry<K, V> r = null;
                if (subiter != null) {
                    r = subiter.next();
                    checkSubiter();
                }
                else {
                    r = current.kvPair();
                    advance();
                }

                lastReturned = r;
                if (r instanceof Entry) {
                    final Entry<K, V> rr = (Entry<K, V>) r;
                    return nextEntry(rr);
                }
                return r;
            }
            else {
                // return Iterator.empty ().next ();
                return null;
            }
        }

        Entry<K, V> nextEntry(final Entry<K, V> rr) {
            return new Entry<K, V>() {
                private V updated = null;

                @Override
                public K getKey() {
                    return rr.getKey();
                }

                @Override
                public V getValue() {
                    return (updated == null) ? rr.getValue() : updated;
                }

                @Override
                public V setValue(V value) {
                    updated = value;
                    return ct.replace(getKey(), value);
                }
            };
        }

        private void readin(INode<K, V> in) {
            MainNode<K, V> m = in.gcasRead(ct);
            if (m instanceof CNode) {
                CNode<K, V> cn = (CNode<K, V>) m;
                depth += 1;
                stack[depth] = cn.array;
                stackpos[depth] = -1;
                advance();
            }
            else if (m instanceof TNode) {
                current = (TNode<K, V>) m;
            }
            else if (m instanceof LNode) {
                subiter = ((LNode<K, V>) m).listmap.iterator();
                checkSubiter();
            }
            else if (m == null) {
                current = null;
            }
        }

        // @inline
        private void checkSubiter() {
            if (!subiter.hasNext()) {
                subiter = null;
                advance();
            }
        }

        // @inline
        void initialize() {
//            assert (ct.isReadOnly ());
            INode<K, V> r = ct.RDCSS_READ_ROOT();
            readin(r);
        }

        void advance() {
            if (depth >= 0) {
                int npos = stackpos[depth] + 1;
                if (npos < stack[depth].length) {
                    stackpos[depth] = npos;
                    BasicNode elem = stack[depth][npos];
                    if (elem instanceof SNode) {
                        current = (SNode<K, V>) elem;
                    }
                    else if (elem instanceof INode) {
                        readin((INode<K, V>) elem);
                    }
                }
                else {
                    depth -= 1;
                    advance();
                }
            }
            else
                current = null;
        }

        protected TrieMapIterator<K, V> newIterator(int _lev, TrieMap<K, V> _ct, boolean _mustInit) {
            return new TrieMapIterator<K, V>(_lev, _ct, _mustInit);
        }

        protected void dupTo(TrieMapIterator<K, V> it) {
            it.level = this.level;
            it.ct = this.ct;
            it.depth = this.depth;
            it.current = this.current;

            // these need a deep copy
            System.arraycopy(this.stack, 0, it.stack, 0, 7);
            System.arraycopy(this.stackpos, 0, it.stackpos, 0, 7);

            // this one needs to be evaluated
            if (this.subiter == null)
                it.subiter = null;
            else {
                List<Entry<K, V>> lst = toList(this.subiter);
                this.subiter = lst.iterator();
                it.subiter = lst.iterator();
            }
        }

        // /** Returns a sequence of iterators over subsets of this iterator.
        // * It's used to ease the implementation of splitters for a parallel
        // version of the TrieMap.
        // */
        // protected def subdivide(): Seq[Iterator[(K, V)]] = if (subiter ne
        // null) {
        // // the case where an LNode is being iterated
        // val it = subiter
        // subiter = null
        // advance()
        // this.level += 1
        // Seq(it, this)
        // } else if (depth == -1) {
        // this.level += 1
        // Seq(this)
        // } else {
        // var d = 0
        // while (d <= depth) {
        // val rem = stack(d).length - 1 - stackpos(d)
        // if (rem > 0) {
        // val (arr1, arr2) = stack(d).drop(stackpos(d) + 1).splitAt(rem / 2)
        // stack(d) = arr1
        // stackpos(d) = -1
        // val it = newIterator(level + 1, ct, false)
        // it.stack(0) = arr2
        // it.stackpos(0) = -1
        // it.depth = 0
        // it.advance() // <-- fix it
        // this.level += 1
        // return Seq(this, it)
        // }
        // d += 1
        // }
        // this.level += 1
        // Seq(this)
        // }

        private List<Entry<K, V>> toList(Iterator<Entry<K, V>> it) {
            ArrayList<Entry<K, V>> list = new ArrayList<Entry<K, V>>();
            while (it.hasNext()) {
                list.add(it.next());
            }
            return list;
        }

        void printDebug() {
            System.out.println("ctrie iterator");
            System.out.println(Arrays.toString(stackpos));
            System.out.println("depth: " + depth);
            System.out.println("curr.: " + current);
            // System.out.println(stack.mkString("\n"));
        }

        @Override
        public void remove() {
            if (lastReturned != null) {
                ct.remove(lastReturned.getKey());
                lastReturned = null;
            }
            else
                throw new IllegalStateException();
        }

    }

    /**
     * Support for EntrySet operations required by the Map interface
     */
    final class EntrySet extends AbstractSet<Entry<K, V>> {

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return TrieMap.this.iterator();
        }

        @Override
        public final boolean contains(final Object o) {
            if (!(o instanceof Entry)) {
                return false;
            }
            final Entry<K, V> e = (Entry<K, V>) o;
            final K k = e.getKey();
            final V v = lookup(k);
            return v != null;
        }

        @Override
        public final boolean remove(final Object o) {
            if (!(o instanceof Entry)) {
                return false;
            }
            final Entry<K, V> e = (Entry<K, V>) o;
            final K k = e.getKey();
            return null != TrieMap.this.remove(k);
        }

        @Override
        public final int size() {
            int size = 0;
            for (final Iterator<?> i = iterator(); i.hasNext(); i.next()) {
                size++;
            }
            return size;
        }

        @Override
        public final void clear() {
            TrieMap.this.clear();
        }
    }
}
