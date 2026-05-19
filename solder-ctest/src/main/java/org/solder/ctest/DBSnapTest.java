package org.solder.ctest;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.solder.dbSnap.SQLDBSnap;

import com.aura.crypto.CryptoScheme;
import com.beech.testing.TableTest;
import com.ee.session.db.EESessionProvider;
import com.lnk.jdbc.SQLDatabase;
import com.lnk.lucene.LBytesRefHash;

public class DBSnapTest {

	private static Log LOG = LogFactory.getLog(DBSnapTest.class.getName());

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
				fileRoot = new File(SolderTestCommon.fileInstall, "logs/DBSnapTest");
				FileUtils.forceMkdir(fileRoot);
				FileUtils.cleanDirectory(fileRoot);
			}

			
			
			SolderTestCommon.initSolder("DBSnapTest.init");
			

			CryptoScheme scheme = CryptoScheme.getDefault();
			
		}
	}
	
	@BeforeAll
	static synchronized void setup() throws IOException {
		LOG.info(String.format("*** Testing %S*****\r\n", TableTest.class.getName()));
		init();

	}
	

	
	public DBSnapTest() {
	}
	
	
	@Test
	public void test_001_DBSnapTSegments() throws Exception {
		
		File fileSnapRoot  =fileRoot;
		SQLDatabase db = EESessionProvider.getDefaultDB();
		SQLDBSnap.addSnapShot(db.getName(),fileSnapRoot,"1.0");
		
		
	}

		
}
