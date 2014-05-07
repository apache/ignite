/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2012, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package com.romix.scala.collection.concurrent;


import java.util.concurrent.atomic.*;


abstract class INodeBase<K, V> extends BasicNode {

    public static final AtomicReferenceFieldUpdater<INodeBase, MainNode> updater = AtomicReferenceFieldUpdater.newUpdater(INodeBase.class, MainNode.class, "mainnode");

    public static final Object RESTART = new Object();
    public final Gen gen;
    public volatile MainNode<K, V> mainnode = null;

    public INodeBase(Gen generation) {
        gen = generation;
    }

    public BasicNode prev() {
        return null;
    }

}