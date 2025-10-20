package org.solder.core;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nimbo.azure.AzureBlobProvider;
import org.nimbo.blobs.ContainerGroup;
import org.solder.vsync.SolderVaultFactory;
import org.solder.vsync.SyncLocalRepo;

import com.ee.session.db.BackgroundTask;
import com.ee.session.db.EESessionProvider;
import com.ee.session.db.Event;
import com.lnk.jdbc.SQLDatabase;
import com.lnk.lucene.RunOnce;

public class SolderMain {
	
	
	private static Log LOG = LogFactory.getLog(SolderMain.class.getName());
	static AtomicBoolean s_fInit = new AtomicBoolean(false);
	static final int CACHE_REFRESH_SECONDS = (int) TimeUnit.MINUTES.toSeconds(10);
	
	static String SOLDER_CONTAINER_GROUP = "drink";
	
	public static void init() throws IOException {
		
		SQLDatabase db = EESessionProvider.getDefaultDB();
		SQLDatabase dbFinal = Objects.requireNonNull(db, "db");
		RunOnce.ensure(s_fInit, () -> {
			StopWatch swInit = new StopWatch("SolderMain.init");
			swInit.start();
			
			//Dependency first... (Runonce helps circular calls).
			ContainerGroup.init();
			AzureBlobProvider.init();
			new SolderVaultFactory();
			SolderVaultFactory.init(dbFinal);
			SyncLocalRepo.initDefault();
			
			// Load everything once...
			syncObjects();
			BackgroundTask.get().createFuture("SolderMain.SyncObjects", (ee) -> {
				return ee.scheduleWithFixedDelay(SolderException.unchecked(SolderMain::syncObjects), CACHE_REFRESH_SECONDS,
						CACHE_REFRESH_SECONDS, TimeUnit.SECONDS);
			});
			
			swInit.stop();
			Event.log(SEvent.SolderMain_Init,-1,-1, (mb)->{
				mb.put("op_time", swInit.getTime());
			});

		});
	}
	
	public static String getSolderContainerGroupName() {
		return SOLDER_CONTAINER_GROUP;
	}
	
	public static void setSolderContainerGroup(ContainerGroup cg) throws IOException{
		Objects.requireNonNull(cg,"container group");
		String prev = SOLDER_CONTAINER_GROUP;
		LOG.info(String.format("Setting Solder Container Group as %s (prev=%s)", cg.getName(),prev));
		SOLDER_CONTAINER_GROUP = cg.getName();
		
		Event.log(SEvent.SolderSetContinerGroup, -1, -1, (mb) -> {
			mb.put("cg", cg.getName());
			mb.put("cg.prev", prev);
		});
	}
	
	static void syncObjects() throws IOException {
		//We removed what we had..
		
		
	}

}
