package org.solder.core;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nimbo.azure.AzureBlobProvider;
import org.nimbo.blobs.CGRegistry;
import org.nimbo.blobs.ContainerGroup;
import org.solder.dbSnap.TSnap;
import org.solder.ens.SolderRestSkeleton;
import org.solder.telemetry.SolderTelemetryWriter;
import org.solder.vsync.SolderVaultFactory;
import org.solder.vsync.SyncLocalRepo;

import com.ee.session.ISession;
import com.ee.session.ITransaction;
import com.ee.session.SessionManager;
import com.ee.session.db.EESessionProvider;
import com.ee.session.db.Event;
import com.ee.util.Config;
import com.ee.util.SessUtil;
import com.lnk.jdbc.SQLDatabase;
import com.lnk.lucene.BackgroundTask;
import com.lnk.lucene.RunOnce;

public class SolderMain {
	
	
	private static Log LOG = LogFactory.getLog(SolderMain.class.getName());
	static AtomicBoolean s_fInit = new AtomicBoolean(false);
	static final int CACHE_REFRESH_SECONDS = (int) TimeUnit.MINUTES.toSeconds(10);
	
	public static String SOLDER_CGREG_NAME = "solder";
	
	
	private static final AtomicReference<ContainerGroup> arSolderCg = new AtomicReference<>();
	
	
	
	public static ContainerGroup getSolderCg() {
		return arSolderCg.get();
	}
	

	
	public static void init(Config cfg) throws IOException {
		
		SQLDatabase db = EESessionProvider.getDefaultDB();
		SQLDatabase dbFinal = Objects.requireNonNull(db, "db");
		RunOnce.ensure(s_fInit, () -> {
			StopWatch swInit = new StopWatch("SolderMain.init");
			swInit.start();
			
			// Dependency first... (Runonce helps circular calls).
			ITransaction t = SessionManager.getTransaction();
			ISession sessCreatedHere = null;
			if (t==null) {
				//t will be null if Solder run stand alone.
				LOG.info(String.format("Initializing System Session in SolderMain. No Previous session"));
				sessCreatedHere = SessionManager.createSystemSession();
				t = sessCreatedHere.beginTrans("SolderMain.init", null, false);
				
			}
			
			
			try {
				// Dependency first... (Runonce helps circular calls).
				ContainerGroup.init();
				AzureBlobProvider.init();
				new SolderVaultFactory();
				SolderVaultFactory.init(dbFinal);
				TSnap.init(dbFinal);
				SyncLocalRepo.initDefault();

				CGRegistry cgReg = CGRegistry.getByName(SOLDER_CGREG_NAME);
				if (cgReg == null) {
					// Create one...
					cgReg = new CGRegistry(SOLDER_CGREG_NAME, "");
				}
				String stCg = cgReg.getGroup();
				if (!StringUtils.isEmpty(stCg)) {
					ContainerGroup cg = ContainerGroup.get(stCg);
					LOG.info(String.format("Solder CgRegistry %s -> Found Conainer Group %s info=%s", cgReg.getName(),
							cgReg.getGroup(), "" + cg));
					if (cg != null) {
						arSolderCg.set(cg);
					}
				}

				// Load everything once...
				syncObjects();
				BackgroundTask.get().createFuture("SolderMain.SyncObjects", (ee) -> {
					return ee.scheduleWithFixedDelay(SessUtil.makeSessFuture("SolderMainSync", SolderMain::syncObjects),
							CACHE_REFRESH_SECONDS, CACHE_REFRESH_SECONDS, TimeUnit.SECONDS);
				});

				// Move this to out -- Ideally to the dedicated servlet that use this.
				// For now everything gets this.
				SolderRestSkeleton.init();

				SolderTelemetryWriter.init();

				swInit.stop();
				Event.log(SEvent.SolderMain_Init, -1, -1, (mb) -> {
					mb.put("op_time", swInit.getTime());
				});
			} finally {
				if (sessCreatedHere != null) {
					try {
						LOG.info(String.format("Ending System Session created in SolderMain."));
						sessCreatedHere.endSession();
					} catch (Exception e) {
						LOG.error(String.format("Error ending SolderMain initSession err=" + e.toString()));
					}
				}
			}

		});
	}
	
	
	
	static void syncObjects() throws IOException {
		
		CGRegistry cgReg = CGRegistry.getByName(SOLDER_CGREG_NAME);
		if (cgReg == null) {
			//Create one...
			cgReg = new CGRegistry(SOLDER_CGREG_NAME,"");
		}
		String stCg  = cgReg.getGroup();
		if (!StringUtils.isEmpty(stCg)) {
			ContainerGroup cg = ContainerGroup.get(stCg);
			LOG.info(String.format("Solder CgRegistry %s -> Found Conainer Group %s info=%s",cgReg.getName(),cgReg.getGroup(),""+cg));
			if (cg != null) {
				arSolderCg.set(cg);
			}
		}
		
	}

}
