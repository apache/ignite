package org.apache.ignite.ml.inference.storage.model;

/**
 * Model storage provider that keeps files and directories presented as {@link FileOrDirectory} files and correspondent
 * locks.
 */
public interface ModelStorageProvider {
    /**
     * Returns file or directory associated with the specified path.
     *
     * @param path Path of file or directory.
     * @return File or directory associated with the specified path.
     */
    public FileOrDirectory get(String path);

    /**
     * Saves file or directory associated with the specified path.
     *
     * @param path Path to the file or directory.
     * @param file File or directory to be saved.
     */
    public void put(String path, FileOrDirectory file);

    /**
     * Removes file or directory associated with the specified path.
     *
     * @param path Path to the file or directory.
     */
    public void remove(String path);

    /**
     * Locks the row associated with the specified path.
     *
     * @param path Path to be locked.
     */
    public void lock(String path);

    /**
     * Unlocks the row associated with the specified path.
     *
     * @param path Path to be unlocked.
     */
    public void unlock(String path);
}
