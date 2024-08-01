package org.shaofan.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.StandardCopyOption;
import java.util.Collection;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IgfsUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(IgfsUtils.class);

	/**
     * Deletes file or directory. If directory
     * is not empty, it's deleted recursively.
     *
     * @param fs IGFS.
     * @param path File or directory path.
     * @throws IgniteException In case of error.
     */
	public static boolean delete(IgniteFileSystem fs, IgfsPath path) throws IgniteException {
        assert fs != null;
        assert path != null;

        if (fs.exists(path)) {
            boolean isFile = fs.info(path).isFile();

            try {
                fs.delete(path, true);
                
                LOGGER.info(">>> Deleted " + (isFile ? "file" : "directory") + ": " + path);
                return true;
            }
            catch (IgfsException e) {                
                LOGGER.error(">>> Failed to delete " + (isFile ? "file" : "directory") + " [path=" + path +
                    ", msg=" + e.getMessage() + ']');
            }
        }
        else {            
        	LOGGER.warn(">>> Won't delete file or directory (doesn't exist): " + path);
        }
        return false;
    }

    /**
     * Creates directories.
     *
     * @param fs IGFS.
     * @param path Directory path.
     * @throws IgniteException In case of error.
     */
	public static void mkdirs(IgniteFileSystem fs, IgfsPath path) throws IgniteException {
        assert fs != null;
        assert path != null;

        try {
            fs.mkdirs(path);            
            LOGGER.info(">>> Created directory: " + path);
        }
        catch (IgfsException e) {
        
            LOGGER.error(">>> Failed to create a directory [path=" + path + ", msg=" + e.getMessage() + ']');
        }
    }

    /**
     * Creates file and writes provided data to it.
     *
     * @param fs IGFS.
     * @param path File path.
     * @param data Data.
     * @throws IgniteException If file can't be created.
     * @throws IOException If data can't be written.
     */
	public static void create(IgniteFileSystem fs, IgfsPath path, byte[] data) throws IgniteException, IOException {
        assert fs != null;
        assert path != null;

        try (OutputStream out = fs.create(path, true)) {
            
        	LOGGER.info(">>> Created file: " + path);

            if (data != null) {
                out.write(data);
                LOGGER.info(">>> Wrote data to file: " + path);
            }
        }
    }
	

    /**
     * Append file and writes provided data to it.
     *
     * @param fs IGFS.
     * @param path File path.
     * @param data Data.
     * @throws IgniteException If file can't be created.
     * @throws IOException If data can't be written.
     */
	public static boolean append(IgniteFileSystem fs, IgfsPath path, InputStream in)  {
        assert fs != null;
        assert path != null;
        byte[] data = new byte[fs.configuration().getBlockSize()]; // 1M    
        try (OutputStream out = fs.create(path, false)) {            
        	LOGGER.info(">>> Created file: " + path);
        	int w = 0;
        	while((w=in.read(data))>0) {        		
        		out.write(data,0,w);        		
        	}
            LOGGER.info(">>> Wrote data to file: " + path);
            return true;
        } catch (IOException e) {			
			e.printStackTrace();
			return false;
		}
        
    }
	

    /**
     * Creates file and writes provided data to it.
     *
     * @param fs IGFS.
     * @param path File path.
     * @param data Data.
     * @throws IgniteException If file can't be created.
     * @throws IOException If data can't be written.
     */
	public static boolean create(IgniteFileSystem fs, IgfsPath path, InputStream in)  {
        assert fs != null;
        assert path != null;
        byte[] data = new byte[fs.configuration().getBlockSize()]; // 1M    
        try (OutputStream out = fs.create(path, true)) {            
        	LOGGER.info(">>> Created file: " + path);
        	int w = 0;
        	while((w=in.read(data))>0) {        		
        		out.write(data,0,w);        		
        	}
            LOGGER.info(">>> Wrote data to file: " + path);
            return true;
        } catch (IOException e) {			
			e.printStackTrace();
			return false;
		}
        
    }
    
    /**
     * Lists files in directory.
     *
     * @param fs IGFS.
     * @param path Directory path.
     * @throws IgniteException In case of error.
     */
	public static Collection<IgfsPath> list(IgniteFileSystem fs, IgfsPath path) throws IgniteException {
        assert fs != null;
        assert path != null;
        assert fs.info(path).isDirectory();

        Collection<IgfsPath> files = fs.listPaths(path);

        if (files.isEmpty()) {            
            LOGGER.warn(">>> No files in directory: " + path);
        }
        else {            
            LOGGER.info(">>> List of files in directory: " + path);
        }
        return files;
    }

    /**
     * Opens file and reads it to byte array.
     *
     * @param fs IgniteFs.
     * @param path File path.
     * @throws IgniteException If file can't be opened.
     * @throws IOException If data can't be read.
     */
	public static byte[] read(IgniteFileSystem fs, IgfsPath path) throws IgniteException, IOException {
        assert fs != null;
        assert path != null;
        assert fs.info(path).isFile();

        byte[] data = new byte[(int)fs.info(path).length()];

        try (IgfsInputStream in = fs.open(path)) {
            in.read(data);
        }
        return data;
    }
	
	public static int pipe(IgniteFileSystem fs, IgfsPath path, OutputStream output) throws IgniteException, IOException {
        assert fs != null;
        assert path != null;
        assert fs.info(path).isFile();

        byte[] data = new byte[fs.configuration().getBlockSize()];
        int len = 0;
        try (IgfsInputStream in = fs.open(path)) {        	
        	int w = 0;
        	while((w=in.read(data))>0) {        		
        		output.write(data,0,w);
        		len += w;
        	}
        }
        return len;
    }
	
	
	public static boolean copy(IgniteFileSystem fs, IgfsPath path, IgfsPath to, StandardCopyOption opt) throws IgniteException, IOException {
        assert fs != null;
        assert path != null;
        assert fs.info(path).isFile();

        byte[] data = new byte[fs.configuration().getBlockSize()];
        int len = 0;
        try (IgfsInputStream in = fs.open(path);
        	OutputStream out = fs.create(to, true)) {        	
        	int w = 0;
        	while((w=in.read(data))>0) {        		
        		out.write(data,0,w);
        		len += w;
        	}
        }       
        return len>0;
    }
	
	
	public static boolean move(IgniteFileSystem fs, IgfsPath path, IgfsPath to, StandardCopyOption opt) throws IgniteException, IOException {
        assert fs != null;
        assert path != null;
        assert fs.info(path).isFile();
        
        if(opt == StandardCopyOption.ATOMIC_MOVE || opt == StandardCopyOption.REPLACE_EXISTING) {
        	try {
        		fs.rename(path, to);
        		return true;
        	}catch(IgniteException e) {
        		// do next code
        	}
        }

        if(copy(fs,path,to,opt)) {
        	fs.delete(path, false);
        	return true;
        }       
        return false;
    }

}
