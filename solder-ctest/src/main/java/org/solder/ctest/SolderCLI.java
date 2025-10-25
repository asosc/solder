package org.solder.ctest;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.commons.cli.Option;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nimbo.blobs.ContainerGroup;
import org.solder.core.SolderException;
import org.solder.core.SolderMain;
import org.solder.vsync.SolderRepoOps;
import org.solder.vsync.SolderVaultFactory;
import org.solder.vsync.SolderRepoOps.SLocalRepo;
import org.solder.vsync.SolderVaultFactory.SRepo;

import com.aura.crypto.CryptoScheme;
import com.ee.session.ISession;
import com.ee.session.SessionManager;
import com.ee.session.db.EESessionProvider;
import com.ee.session.db.Tenant;
import com.jnk.junit.AbstractCLI;
import com.jnk.util.CompareUtils;
import com.jnk.util.PrintUtils;
import com.jnk.util.TypeConversion;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.jnk.util.random.IRandom;
import com.lnk.lucene.TempFiles;

public class SolderCLI  extends AbstractCLI {

	static Log LOG = LogFactory.getLog(SolderCLI.class.getName());

	public SolderCLI() {
		super("SolderCLI");

		// Op incluse [list,extract]

		options.addOption(
				new Option("g", "git", true, String.format("Git: op opParams\r\n%s", Arrays.toString(git_Ops))));
	

	}

	void init() throws IOException {
		// Initialize all things.
		TempFiles.initDefault();
	}

	public boolean doRunMain() throws Exception {

		init();

		String[] args = cline.getArgs();
		int nParam = 0;

		if (cline.hasOption("git")) {
			System.out.println("Git command");
			String op = cline.getOptionValue("git");

			try (GitCmdHandler handler = new GitCmdHandler(op, args, nParam)) {
				handler.doOp();
			}
			return true;
		} else {
			System.out.println(" NO OPTIONS FOUND ON AbstractCLI");
			printHelp();
			return true;
		}

	}
	
	public void printHelp() {
		super.printHelp();
		System.out.println("**********Op Details************\r\n");
		System.out.println("**********Git Ops************\r\n");
		for (var entry : mapGitOpsHelp.entrySet()) {
			System.out.println(String.format("%s ->%s", entry.getKey(),entry.getValue()));
		}
		System.out.println("**********End Git Ops************\r\n");
	}

	public static void main(String[] a) {
		SolderCLI cli = new SolderCLI();
		cli.runMain(a, true);
	}

	// ALL Handlers are here..

	static final String[] git_Ops = { "create","checkout","push"};
	static final TreeMap<String,String> mapGitOpsHelp = new TreeMap<>();
	static {
		mapGitOpsHelp.put("create",
				"Git create. Params:EEDBCfgFile fileLocalRepo repoId schemaName [tenant_id aoId]");
		mapGitOpsHelp.put("checkout",
				"Git Checkout(same as clone,rebase). Params:EEDBCfgFile");
		mapGitOpsHelp.put("push",
				"Git Push(sam as commit and push). Params:EEDBCfgFile");
		mapGitOpsHelp.put("init",
				"Git init. Params:EEDBCfgFile repoId");
	}
	
	
	static void initSolder(File fileCfg,String stCmd) throws IOException {
		Validator.checkFile(fileCfg, "Enigma DB file to store.");
		logConsole("Initializing EESessionProvider using config "+fileCfg.getAbsolutePath());
		EESessionProvider.init(fileCfg);
		logConsole("Success initializing EESessionProvider using config "+fileCfg.getAbsolutePath());
		ISession s = SessionManager.createSystemSession();
		s.beginTrans("InitSolder", null, false);
		SolderMain.init();
		s.endSession();
		if (stCmd != null && stCmd.length()>0) {
			ISession s2 = SessionManager.createSystemSession();
			s2.beginTrans(stCmd, null, false);
		}
		
	}

	

	//static final String[] mapGitOpsHelp = { "create" }
	class GitCmdHandler implements Closeable {

		String op;
		String[] args;
		int nParam;


		GitCmdHandler(String op, String[] args, int nParam) throws IOException {
			if (op == null || op.length() == 0) {
				op = git_Ops[0];
			}
			this.args = args;
			this.nParam = nParam;
			this.op = op.toLowerCase();
		}

		void doOp() throws IOException {

			// We need vaultFactory Param.
			switch (op) {
			case "create":
			{
				File fileCfg = makeFile(args[nParam++]);
				initSolder(fileCfg,"SolderCLIGitCreate");
				File fileCache = makeFile(args[nParam++]);
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				
				IRandom r = CryptoScheme.getDefault().getRandom();
				String stId = args[nParam++];
				String schemaName = args[nParam++];
				
				int tenantId = nParam>args.length?TypeConversion.asInt(args[nParam++]):Tenant.ROOT_ID;
				int aoId = nParam>args.length?TypeConversion.asInt(args[nParam++]):Math.abs(r.nextInt());
				
				logConsole("id: "+stId+"; schema="+schemaName);
				gitCreate(fileCache,stId,schemaName,tenantId,aoId);
			}
			break;
			
			case "init": {
				File fileCfg = makeFile(args[nParam++]);
				initSolder(fileCfg,"SolderCLIGitCreate");
				File fileCache = makeFile("");
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				String stId = args[nParam++];
				gitInit(fileCache,stId);
				break;
			}
			
			case "checkout": {
				File fileCfg = makeFile(args[nParam++]);
				initSolder(fileCfg,"SolderCLIGitCreate");
				File fileCache = makeFile("");
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				gitCheckout(fileCache);
				break;
			}
			
			case "push": {
				File fileCfg = makeFile(args[nParam++]);
				initSolder(fileCfg,"SolderCLIGitCreate");
				File fileCache = makeFile("");
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				
				gitPush(fileCache);
				break;
			}
			
			default: {
				logConsole("Unknown Op for git " + op);
				throw new SolderException("Unknown Op for git " + op);
			}
			}

		}

		public void close() {
			
		}
		
		
	
		void gitCreate(File fileCache,String stId,String schemaName,int tenantId,int aoId)  throws IOException {
			
			
			stId = Validator.require(stId,"id ",Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			schemaName = Validator.require(schemaName,"schemaName ",Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			
			logConsole(String.format("Create Solder Rep %s (schema=%s) using dir %s",stId,schemaName,fileCache.getAbsolutePath()));
			
			
			String stCommitDir = "Commits";
			String[] aExt = new String[] {"bee"};
			
			//Ensure the record is there..
			SRepo repo = SolderVaultFactory.getRepoById(stId);
			if (repo==null) {
				logConsole(String.format("No previous repo found. creating.."));
				repo = new SRepo(stId,schemaName,tenantId,aoId,stCommitDir,aExt);
			} else {
				logConsole(String.format("Found previous repo found. commitId=%d (date=%s) ",repo.getCommitId(),PrintUtils.print(repo.getCommitDate())));
			}
			
			SolderRepoOps.repInit(repo, fileCache);
			SolderRepoOps.repCommit(repo, fileCache,(mapCommit)->{
				mapCommit.put("cmsg", "SolderCLI gitCreate");
			});
			
		}
		
		
		void gitInit(File fileCache,String stId)  throws IOException {
			
			
			
			stId = Validator.require(stId,"id ",Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			
			
			logConsole(String.format("Git Init Solder Rep %s using dir %s",stId,fileCache.getAbsolutePath()));
			
			
						
			//Ensure the record is there..
			SRepo repo = SolderVaultFactory.getRepoById(stId);
			if (repo==null) {
				throw new SolderException("Unknown repo id "+stId);
			} else {
				logConsole(String.format("Found previous repo found. commitId=%d (date=%s) ",repo.getCommitId(),PrintUtils.print(repo.getCommitDate())));
			}
			SolderRepoOps.repInit(repo, fileCache);
			
		}
		
		
		void gitCheckout(File fileCache)  throws IOException {
			
			String stId = SolderRepoOps.readLocalRepoId(fileCache);
			
			stId = Validator.require(stId,"id ",Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			
			
			logConsole(String.format("Checout Solder Rep %s using dir %s",stId,fileCache.getAbsolutePath()));
			
			//Ensure the record is there..
			SRepo repo = SolderVaultFactory.getRepoById(stId);
			if (repo==null) {
				logConsole(String.format("Unknown repoId "+stId));
			} else {
				logConsole(String.format("Found repo. commitId=%d (date=%s) ",repo.getCommitId(),PrintUtils.print(repo.getCommitDate())));
			}
			
			SLocalRepo lrepo = new SLocalRepo(repo, fileCache,false);
			SolderRepoOps.repoCheckout(lrepo);
			logConsole(String.format("Done checkout"));
		}
		
		void gitPush(File fileCache)  throws IOException {
			
	
			String stId = SolderRepoOps.readLocalRepoId(fileCache);
			stId = Validator.require(stId,"id ",Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			
			logConsole(String.format("Checout Solder Rep %s using dir %s",stId,fileCache.getAbsolutePath()));
			
			//Ensure the record is there..
			SRepo repo = SolderVaultFactory.getRepoById(stId);
			if (repo==null) {
				logConsole(String.format("Unknown repoId "+stId));
			} else {
				logConsole(String.format("Found repo. commitId=%d (date=%s) ",repo.getCommitId(),PrintUtils.print(repo.getCommitDate())));
			}
			
			
			SolderRepoOps.repCommit(repo,fileCache,(mapCommit)->{
				mapCommit.put("cmsg", "SolderCLI gitpush");
			});
			logConsole(String.format("Done push"));
		}

	}
}