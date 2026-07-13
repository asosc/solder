package org.solder.ctest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.core.SolderMain;

import com.ee.session.ISession;
import com.ee.session.SessionManager;
import com.ee.session.db.EESessionProvider;
import com.jnk.junit.ResourceUtil;
import com.jnk.util.Validator;
import com.lnk.lucene.LBytesRef;
import com.lnk.lucene.LBytesRefBuilder;
import com.lnk.lucene.LBytesRefHash;
import com.lnk.lucene.LuceneUtilException;
import com.lnk.lucene.TempFiles;

public class SolderTestCommon {

	
	private static Log LOG = LogFactory.getLog(SolderTestCommon.class.getName());
	
	static AtomicBoolean s_fInit = new AtomicBoolean(false);
	static Random random = new Random();
	static LBytesRefHash s_refHashRivers = null;
	static File fileInstall = null;
	
	static int[] s_MONTH_DAYS = null;
	

	public static LBytesRefHash getRiverNames() throws IOException {
		InputStream is = ResourceUtil.getResourceAsStream(ResourceUtil.class, "us_rivers.txt");
		try {
			LBytesRefBuilder brb = new LBytesRefBuilder();
			brb.append(is, -1, true);
			LBytesRef br = brb.get();
			String[] a = StringUtils.split(br.utf8ToString(), '\n');
			LBytesRefHash refHash = new LBytesRefHash();
			for (String st : a) {
				st = st.trim();
				if (st.length() > 0 && !st.startsWith("//")) {
					refHash.add(st);
				}
			}
			return refHash;
		} finally {
			IOUtils.closeQuietly(is);
		}
	}
	
	static synchronized void init() throws IOException {
		if (!s_fInit.get()) {
			s_refHashRivers = getRiverNames();

			Map<String, String> mapEnv = System.getenv();
			String stLogRoot = mapEnv.get("ENIGMA_INSTALL");

			if (stLogRoot == null) {
				stLogRoot = "";
			}
			
			fileInstall = new File(stLogRoot);
			FileUtils.forceMkdir(fileInstall);
			
			File fileTemp = new File(stLogRoot, "logs/temp");
			TempFiles.ensure(TempFiles.DEFAULT,()->{
				try {
					return new TempFiles(TempFiles.DEFAULT, fileTemp);
				}catch(IOException e) {
					throw LuceneUtilException.rethrowUnchecked(e);
				}
			});
		
			s_MONTH_DAYS = new int[] { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
			
			s_fInit.set(true);
		}
	}
	
	public static void initSolder(String stCmd) throws IOException {
		Objects.requireNonNull(fileInstall,"fileInstall");
		File fileCfg = new File(fileInstall,"ens/WEB-INF/ens.cfg");
		Validator.checkFile(fileCfg, "Enigma DB file to store.");
		
		LOG.info("Initializing EESessionProvider using config "+fileCfg.getAbsolutePath());
		EESessionProvider.init(fileCfg);
		LOG.info("Success initializing EESessionProvider using config "+fileCfg.getAbsolutePath());
		
		ISession s = SessionManager.createSystemSession();
		s.beginTrans("InitSolder", null, false);
		SolderMain.init();
		
		s.endSession();
		if (stCmd != null && stCmd.length()>0) {
			ISession s2 = SessionManager.createSystemSession();
			s2.beginTrans(stCmd, null, false);
		}
		
	}
	
	

}
