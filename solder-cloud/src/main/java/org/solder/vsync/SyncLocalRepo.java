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

public class SyncLocalRepo {


	private static Log LOG = LogFactory.getLog(SyncLocalRepo.class.getName());
	
	
	//NO LifeCycle management .. We want apps to do this anyway.

	
	public static final String DEFAULT = "sdefault";
	
	
	
	private static final NamedSPI<SyncLocalRepo> REGISTERED = new NamedSPI<SyncLocalRepo>(true);
	
	
	public static synchronized void initDefault() throws IOException {
		if (!REGISTERED.availableServices().contains(DEFAULT)) {
			Map<String, String> mapEnv = System.getenv();
			String stSyncRoot = mapEnv.get("ENIGMA_INSTALL");

			if (stSyncRoot == null) {
				stSyncRoot = "";
			}
	
			File fileSyncCache = new File(stSyncRoot, "syncLocalRepo");
			ensure(TempFiles.DEFAULT,()->{
				try {
					return new SyncLocalRepo(SyncLocalRepo.DEFAULT, fileSyncCache);
				}catch(IOException e) {
					throw LuceneUtilException.rethrowUnchecked(e);
				}
			});
		}
	}

	public static SyncLocalRepo get(String name) {
		return REGISTERED.lookup(name);
	}
	
	
	public static SyncLocalRepo ensure(String name,Supplier<SyncLocalRepo> fn) {
		return REGISTERED.ensure(name, fn);
	}
	
	String name;
	File fileCacheRoot;
	String stCacheRoot; 
	
	
	
	
	public SyncLocalRepo(String name,File fileRoot) throws IOException {
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
	
