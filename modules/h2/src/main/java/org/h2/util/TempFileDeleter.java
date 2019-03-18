/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
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
    private final HashMap<PhantomReference<?>, String> refMap = new HashMap<>();

    private TempFileDeleter() {
        // utility class
    }

    public static TempFileDeleter getInstance() {
        return new TempFileDeleter();
    }

    /**
     * Add a file to the list of temp files to delete. The file is deleted once
     * the file object is garbage collected.
     *
     * @param fileName the file name
     * @param file the object to monitor
     * @return the reference that can be used to stop deleting the file
     */
    public synchronized Reference<?> addFile(String fileName, Object file) {
        IOUtils.trace("TempFileDeleter.addFile", fileName, file);
        PhantomReference<?> ref = new PhantomReference<>(file, queue);
        refMap.put(ref, fileName);
        deleteUnused();
        return ref;
    }

    /**
     * Delete the given file now. This will remove the reference from the list.
     *
     * @param ref the reference as returned by addFile
     * @param fileName the file name
     */
    public synchronized void deleteFile(Reference<?> ref, String fileName) {
        if (ref != null) {
            String f2 = refMap.remove(ref);
            if (f2 != null) {
                if (SysProperties.CHECK) {
                    if (fileName != null && !f2.equals(fileName)) {
                        DbException.throwInternalError("f2:" + f2 + " f:" + fileName);
                    }
                }
                fileName = f2;
            }
        }
        if (fileName != null && FileUtils.exists(fileName)) {
            try {
                IOUtils.trace("TempFileDeleter.deleteFile", fileName, null);
                FileUtils.tryDelete(fileName);
            } catch (Exception e) {
                // TODO log such errors?
            }
        }
    }

    /**
     * Delete all registered temp files.
     */
    public void deleteAll() {
        for (String tempFile : new ArrayList<>(refMap.values())) {
            deleteFile(null, tempFile);
        }
        deleteUnused();
    }

    /**
     * Delete all unused files now.
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
     * This method is called if a file should no longer be deleted if the object
     * is garbage collected.
     *
     * @param ref the reference as returned by addFile
     * @param fileName the file name
     */
    public void stopAutoDelete(Reference<?> ref, String fileName) {
        IOUtils.trace("TempFileDeleter.stopAutoDelete", fileName, ref);
        if (ref != null) {
            String f2 = refMap.remove(ref);
            if (SysProperties.CHECK) {
                if (f2 == null || !f2.equals(fileName)) {
                    DbException.throwInternalError("f2:" + f2 +
                            " " + (f2 == null ? "" : f2) + " f:" + fileName);
                }
            }
        }
        deleteUnused();
    }

}
