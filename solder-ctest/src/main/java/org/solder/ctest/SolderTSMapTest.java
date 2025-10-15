package org.solder.ctest;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.solder.vsync.SolderVaultFactory;
import org.solder.vsync.SolderVaultFactory.SVault;

import com.aura.crypto.CryptoScheme;
import com.beech.store.TVault;
import com.beech.testing.TSMapTest;
import com.ee.session.db.Tenant;
import com.lnk.jdbc.MSSQLUtil;
import com.lnk.jdbc.SQLQuery;
import com.lnk.lucene.LBytesRefHash;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class SolderTSMapTest {

	private static Log LOG = LogFactory.getLog(SolderTSMapTest.class.getName());

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
				fileRoot = new File(SolderTestCommon.fileInstall, "logs/SolderTSMapTest");
				FileUtils.forceMkdir(fileRoot);
				FileUtils.cleanDirectory(fileRoot);
			}

			TSMapTest.init();
			
			SolderTestCommon.initSolder("SolderTSMapTest.init");
			
			File fileCreate = new File(fileRoot, "SolderCreate.SQL");
			File fileDrop = new File(fileRoot, "SolderDrop.SQL");
			File fileQuery = new File(fileRoot, "SolderQuery.txt");
			MSSQLUtil.printAllSchema(fileCreate, fileDrop);
			SQLQuery.printAll(fileQuery);

			CryptoScheme scheme = CryptoScheme.getDefault();
			String id = scheme.getUUID();

			List<Tenant> list = Tenant.getAll();
			int idTenant = Tenant.ROOT_ID;
			if (list.size() > 0) {
				idTenant = list.get(random.nextInt(list.size())).getId();
			}

			SVault svault = new SVault(id, "river_tsmap", idTenant, random.nextInt());

			TSMapTest.setTVault((mode) -> new TVault(SolderVaultFactory.TYPE, id, mode));

		}
	}
	
	@BeforeAll
	static synchronized void setup() throws IOException {
		LOG.info(String.format("*** Testing %S*****\r\n", SolderTSMapTest.class.getName()));
		init();

	}
	
	TSMapTest tsmapTest;
	
	public SolderTSMapTest() {
		tsmapTest = new TSMapTest();
	}
	
	
	
	
	@Test
	public void test_001_OneRiver() throws Exception {
		tsmapTest.test_001_OneRiver();
	}

	@Test
	public void test_002_OneRiverNoIndex() throws Exception {
		tsmapTest.test_002_OneRiverNoIndex();
	}

	@Test
	public void test_003_QuickRiverPair() throws Exception {
		tsmapTest.test_003_QuickRiverPair();
	}
	
	//@Test
	public void test_004_LargeTest() throws Exception {
		tsmapTest.test_004_LargeTest();
	}
	
}
