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
import org.solder.vsync.SolderVaultFactory.SRepo;

import com.aura.crypto.CryptoScheme;
import com.beech.store.TVault;
import com.beech.testing.TableTest;
import com.ee.session.db.Tenant;
import com.lnk.jdbc.MSSQLUtil;
import com.lnk.jdbc.SQLQuery;
import com.lnk.lucene.LBytesRefHash;


@TestMethodOrder(MethodOrderer.MethodName.class)
public class SolderTableTest {

	private static Log LOG = LogFactory.getLog(SolderTableTest.class.getName());

	static Random random = new Random();
	static LBytesRefHash s_refHashRivers = null;
	static File fileRoot = null;
	
	static AtomicBoolean s_fInit = new AtomicBoolean(false);
	
	static String repoId=null;

	static synchronized void init() throws IOException {
		if (!s_fInit.get()) {
			s_fInit.set(true);
			
			SolderTestCommon.init();
			if (fileRoot == null) {
				s_refHashRivers = SolderTestCommon.s_refHashRivers;
				fileRoot = new File(SolderTestCommon.fileInstall, "logs/SolderTableTest");
				FileUtils.forceMkdir(fileRoot);
				FileUtils.cleanDirectory(fileRoot);
			}

			TableTest.init();
			
			SolderTestCommon.initSolder("SolderTableTest.init");
			
			File fileCreate = new File(fileRoot, "SolderCreate.SQL");
			File fileDrop = new File(fileRoot, "SolderDrop.SQL");
			File fileQuery = new File(fileRoot, "SolderQuery.txt");
			MSSQLUtil.printAllSchema(fileCreate, fileDrop);
			SQLQuery.printAll(fileQuery);

			CryptoScheme scheme = CryptoScheme.getDefault();
			repoId = scheme.getUUID();

			List<Tenant> list = Tenant.getAll();
			int idTenant = Tenant.ROOT_ID;
			if (list.size() > 0) {
				idTenant = list.get(random.nextInt(list.size())).getId();
			}

			SRepo srepo = SolderVaultFactory.createBeechSRepo(repoId, "river", idTenant, random.nextInt());

			TableTest.setTVault((mode) -> new TVault(SolderVaultFactory.TYPE, repoId, mode));

		}
	}
	
	@BeforeAll
	static synchronized void setup() throws IOException {
		LOG.info(String.format("*** Testing %S*****\r\n", TableTest.class.getName()));
		init();

	}
	
	TableTest tt;
	
	public SolderTableTest() {
		tt = new TableTest();
	}
	
	
	@Test
	public void test_001_OneRiver() throws Exception {
		tt.test_001_OneRiver();
		SolderVaultFactory svf = (SolderVaultFactory)TVault.getFactory(SolderVaultFactory.TYPE);
		svf.repoGitPush(repoId);
	}

	@Test
	public void test_002_OneRiverNoIndex() throws Exception {
		tt.test_002_OneRiverNoIndex();
		SolderVaultFactory svf = (SolderVaultFactory)TVault.getFactory(SolderVaultFactory.TYPE);
		svf.repoGitPush(repoId);
	}

	@Test
	public void test_003_QuickRiverPair() throws Exception {
		tt.test_003_QuickRiverPair();
		SolderVaultFactory svf = (SolderVaultFactory)TVault.getFactory(SolderVaultFactory.TYPE);
		svf.repoGitPush(repoId);

	}

	//@Test
	public void test_04_LargeRiverTest() throws Exception {
		tt.test_04_LargeRiverTest();
		SolderVaultFactory svf = (SolderVaultFactory)TVault.getFactory(SolderVaultFactory.TYPE);
		svf.repoGitPush(repoId);
	}
	
}
