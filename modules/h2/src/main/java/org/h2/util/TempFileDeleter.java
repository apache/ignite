/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.HashMap;

import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.fs.FileUtils;

/**
 * This class deletes temporary files when they are not used any longer.
 */
public class TempFileDeleter {

    private final ReferenceQueue<Object> queue = new ReferenceQueue<>();
    private final HashMap<PhantomReference<?>, Object> refMap = new HashMap<>();

    private TempFileDeleter() {
        // utility class
    }

    public static TempFileDeleter getInstance() {
        return new TempFileDeleter();
    }

    /**
     * Add a file or a closeable to the list of temporary objects to delete. The
     * file is deleted once the file object is garbage collected.
     *
     * @param resource the file name or the closeable
     * @param monitor the object to monitor
     * @return the reference that can be used to stop deleting the file or closing the closeable
     */
    public synchronized Reference<?> addFile(Object resource, Object monitor) {
        if (!(resource instanceof String) && !(resource instanceof AutoCloseable)) {
            throw DbException.getUnsupportedException("Unsupported resource " + resource);
        }
        IOUtils.trace("TempFileDeleter.addFile",
                resource instanceof String ? (String) resource : "-", monitor);
        PhantomReference<?> ref = new PhantomReference<>(monitor, queue);
        refMap.put(ref, resource);
        deleteUnused();
        return ref;
    }

    /**
     * Delete the given file or close the closeable now. This will remove the
     * reference from the list.
     *
     * @param ref the reference as returned by addFile
     * @param resource the file name or closeable
     */
    public synchronized void deleteFile(Reference<?> ref, Object resource) {
        if (ref != null) {
            Object f2 = refMap.remove(ref);
            if (f2 != null) {
                if (SysProperties.CHECK) {
                    if (resource != null && !f2.equals(resource)) {
                        DbException.throwInternalError("f2:" + f2 + " f:" + resource);
                    }
                }
                resource = f2;
            }
        }
        if (resource instanceof String) {
            String fileName = (String) resource;
            if (FileUtils.exists(fileName)) {
                try {
                    IOUtils.trace("TempFileDeleter.deleteFile", fileName, null);
                    FileUtils.tryDelete(fileName);
                } catch (Exception e) {
                    // TODO log such errors?
                }
            }
        } else if (resource instanceof AutoCloseable) {
            AutoCloseable closeable = (AutoCloseable) resource;
            try {
                IOUtils.trace("TempFileDeleter.deleteCloseable", "-", null);
                closeable.close();
            } catch (Exception e) {
                // TODO log such errors?
            }
        }
    }

    /**
     * Delete all registered temp resources.
     */
    public void deleteAll() {
        for (Object resource : new ArrayList<>(refMap.values())) {
            deleteFile(null, resource);
        }
        deleteUnused();
    }

    /**
     * Delete all unused resources now.
     */
    public void deleteUnused() {
        while (queue != null) {
            Reference<?> ref = queue.poll();
            if (ref == null) {
                break;
            }
            deleteFile(ref, null);
        }
    }

    /**
     * This method is called if a file should no longer be deleted or a resource
     * should no longer be closed if the object is garbage collected.
     *
     * @param ref the reference as returned by addFile
     * @param resource file name or closeable
     */
    public void stopAutoDelete(Reference<?> ref, Object resource) {
        IOUtils.trace("TempFileDeleter.stopAutoDelete",
                resource instanceof String ? (String) resource : "-", ref);
        if (ref != null) {
            Object f2 = refMap.remove(ref);
            if (SysProperties.CHECK) {
                if (f2 == null || !f2.equals(resource)) {
                    DbException.throwInternalError("f2:" + f2 +
                            " " + (f2 == null ? "" : f2) + " f:" + resource);
                }
            }
        }
        deleteUnused();
    }

}
