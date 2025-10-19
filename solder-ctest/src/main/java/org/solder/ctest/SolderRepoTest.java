package org.solder.ctest;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.nimbo.blobs.ContainerGroup;
import org.solder.core.SolderMain;
import org.solder.vsync.SolderVaultFactory;
import org.solder.vsync.SolderVaultFactory.SRepo;

import com.aura.crypto.CryptoScheme;
import com.beech.testing.TableTest;
import com.ee.session.db.Tenant;
import com.lnk.lucene.LBytesRefHash;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class SolderRepoTest {
	
	
	private static Log LOG = LogFactory.getLog(SolderTableTest.class.getName());

	static Random random = new Random();
	static LBytesRefHash s_refHashRivers = null;
	static File fileRoot = null;
	
	static AtomicBoolean s_fInit = new AtomicBoolean(false);

	static synchronized void init() throws IOException {
		if (!s_fInit.get()) {
			s_fInit.set(true);
			
			SolderTestCommon.init();
			if (fileRoot == null) {
				s_refHashRivers = SolderTestCommon.s_refHashRivers;
				fileRoot = new File(SolderTestCommon.fileInstall, "logs/SolderReplicationTest");
				FileUtils.forceMkdir(fileRoot);
				FileUtils.cleanDirectory(fileRoot);
			}

			TableTest.init();
			
			SolderTestCommon.initSolder("SolderReplicationTest.init");
			

			CryptoScheme scheme = CryptoScheme.getDefault();
			String id = scheme.getUUID();

			List<Tenant> list = Tenant.getAll();
			int idTenant = Tenant.ROOT_ID;
			if (list.size() > 0) {
				idTenant = list.get(random.nextInt(list.size())).getId();
			}
		}
	}
	
	@BeforeAll
	static synchronized void setup() throws IOException {
		LOG.info(String.format("*** Testing %S*****\r\n", SolderRepoTest.class.getName()));
		init();

	}
	
	
	@Test
	public void test_001_replicate() throws Exception {
		
		//Replicate all (if a local cache is found).
		
		String[] aStSchema = new String[] {"river","river_tsmap","river_trefhash"};
		
		FileFilter filter = (file) -> {
			String ext = FilenameUtils.getExtension(file.getName());
			return ext != null && ext.equalsIgnoreCase("bee");
		};
		
		String cgName="drink";
		ContainerGroup cg = ContainerGroup.get(cgName);
		SolderMain.setSolderContainerGroup(cg);
		
		for (String stSchema : aStSchema) {
			List<SRepo> list = SolderVaultFactory.selectBySchema(stSchema);
			LOG.info(String.format("Found %d vaults for schema",list.size(),stSchema));
			
			for (SRepo svault : list) {
				//Replicate first..
				svault.getProvider(true);
				
			}
		}
	}

}
