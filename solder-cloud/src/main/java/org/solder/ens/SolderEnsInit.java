package org.solder.ens;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.solder.core.SolderMain;

import com.ee.ens.EnigmaRestSkeleton;
import com.ee.util.Config;

public class SolderEnsInit {

	
	static AtomicBoolean s_fInit = new AtomicBoolean(false);
	public SolderEnsInit(Config cfg) throws IOException{
		
		synchronized(s_fInit) {
			if (s_fInit.get()) {
				throw new RuntimeException("SolderEnsInit already initialized! This need be called by EnServlet during its initialization!");
			}
			s_fInit.set(true);
			SolderMain.init(cfg);
			EnigmaRestSkeleton.init();
			SolderRestSkeleton.init();
		}
		
	}
	
	

}