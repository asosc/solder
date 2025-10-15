package org.solder.vsync;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lnk.lucene.LuceneUtilException;
import com.lnk.lucene.NamedSPI;
import com.lnk.lucene.TempFiles;

public class SyncCache {


	private static Log LOG = LogFactory.getLog(SyncCache.class.getName());
	
	
	//NO LifeCycle management .. We want apps to do this anyway.

	
	public static final String DEFAULT = "sdefault";
	
	
	
	private static final NamedSPI<SyncCache> REGISTERED = new NamedSPI<SyncCache>(true);
	
	
	public static synchronized void initDefault() throws IOException {
		if (!REGISTERED.availableServices().contains(DEFAULT)) {
			Map<String, String> mapEnv = System.getenv();
			String stSyncRoot = mapEnv.get("SOLDER_INSTALL");

			if (stSyncRoot == null) {
				stSyncRoot = "";
			}
	
			File fileSyncCache = new File(stSyncRoot, "syncCache");
			ensure(TempFiles.DEFAULT,()->{
				try {
					return new SyncCache(SyncCache.DEFAULT, fileSyncCache);
				}catch(IOException e) {
					throw LuceneUtilException.rethrowUnchecked(e);
				}
			});
		}
	}

	public static SyncCache get(String name) {
		return REGISTERED.lookup(name);
	}
	
	
	public static SyncCache ensure(String name,Supplier<SyncCache> fn) {
		return REGISTERED.ensure(name, fn);
	}
	
	String name;
	File fileCacheRoot;
	String stCacheRoot; 
	
	
	
	
	public SyncCache(String name,File fileRoot) throws IOException {
		if (REGISTERED.availableServices().contains(name)) {
			throw new RuntimeException("TempFiles "+name+" already registered!");
		}
		
		FileUtils.forceMkdir(fileRoot);
		fileCacheRoot = new File(fileRoot,name);
		stCacheRoot = fileCacheRoot.getAbsolutePath();
		
		FileUtils.forceMkdir(fileCacheRoot);
		REGISTERED.register(name, this);
	}
	
	
	/**
	 * Provides the same sync location on the machine...
	 * 
	 * @param id
	 * @return
	 * @throws IOException
	 */
	public File ensureSyncFolder(String id) throws IOException{
		File file = new File(fileCacheRoot,id);
		FileUtils.forceMkdir(file);
		return file;
	}
	
	
	
	
}
	
