package org.solder.ctest;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.cli.Option;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.core.SolderException;
import org.solder.core.SolderMain;
import org.solder.rest.client.SolderGitClient;
import org.solder.vsync.ServerRepoFileService;
import org.solder.vsync.SolderVaultFactory;
import org.solder.vsync.SolderVaultFactory.SRepo;

import com.aura.crypto.CryptoScheme;
import com.ee.session.ISession;
import com.ee.session.SessionManager;
import com.ee.session.db.EESessionProvider;
import com.ee.session.db.Tenant;
import com.jnk.junit.AbstractCLI;
import com.jnk.util.PrintUtils;
import com.jnk.util.TypeConversion;
import com.jnk.util.Validator;
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

	static final String[] git_Ops = { "create","checkout","push","init","status","search","delete"};
	static final TreeMap<String,String> mapGitOpsHelp = new TreeMap<>();
	static {
		mapGitOpsHelp.put("create", 
				"Git create. Params: fileLocalRepo repoId schemaName [tenant_id aoId]");
		mapGitOpsHelp.put("search", 
				"Search Repo. Params: repoIdPattern schemaNamePattern [tenant_id]");
		mapGitOpsHelp.put("delete", 
				"Delete Repo. Params: repoId");
		
		mapGitOpsHelp.put("init",
				"Git init. Params:repoId");
		
		mapGitOpsHelp.put("checkout",
				"Git Checkout(same as clone,rebase). Params:");
		mapGitOpsHelp.put("push",
				"Git Push(same as commit and push). Params:");
		
		mapGitOpsHelp.put("status",
				"Git Status. Params:");
	}
	
	private static SolderGitClient gitClient = null;
	void initSolder(String stCmd,File fileCache,String repoId) throws IOException {
		
		Map<String, String> mapEnv = System.getenv();
		String stInstall = mapEnv.get("ENIGMA_INSTALL");
		File fileInstall = makeFile(stInstall);
		File fileEECfg = new File(fileInstall,"ens/WEB-INF/ens.cfg");
		logConsole("Initializing EESessionProvider using config "+fileEECfg.getAbsolutePath());
		Validator.checkFile(fileEECfg, "Given config file");
		
		EESessionProvider.init(fileEECfg);
		logConsole("Success initializing EESessionProvider using config "+fileEECfg.getAbsolutePath());
		ISession s = SessionManager.createSystemSession();
		s.beginTrans("InitSolder", null, false);
		SolderMain.init();
		s.endSession();
		if (stCmd != null && stCmd.length()>0) {
			ISession s2 = SessionManager.createSystemSession();
			s2.beginTrans(stCmd, null, false);
		}
		if (fileCache != null) {
			gitClient = new SolderGitClient(ServerRepoFileService.get(),fileCache,repoId,(st)->logConsole(st));
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
				
				initSolder("SolderCLIGitCreate",null,null);
				File fileCache = makeFile(args[nParam++]);
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				
				IRandom r = CryptoScheme.getDefault().getRandom();
				String repoId = args[nParam++];
				String schemaName = args[nParam++];
				
				int tenantId = nParam>args.length?TypeConversion.asInt(args[nParam++]):Tenant.ROOT_ID;
				int aoId = nParam>args.length?TypeConversion.asInt(args[nParam++]):Math.abs(r.nextInt());
				
				logConsole("id: "+repoId+"; schema="+schemaName);
				
				String stCommitDir = "Commits";
				String[] aExt = new String[] {"bee"};
				SRepo repo = SolderVaultFactory.getRepoById(repoId);
				if (repo==null) {
					logConsole(String.format("No previous repo found. creating.."));
					repo = new SRepo(repoId,schemaName,tenantId,aoId,stCommitDir,aExt);
				} else {
					logConsole(String.format("Found previous repo found. commitId=%d (date=%s) ",repo.getCommitId(),PrintUtils.print(repo.getCommitDate())));
				}
				
			}
			break;
			
			case "search":
			{
				initSolder("SolderCLIGitSearch",null,null);
				
				String repoIdPattern = nParam<args.length?args[nParam++]:"";
				String schemaNamePattern =  nParam<args.length?args[nParam++]:"";
				int tenantId = nParam<args.length?TypeConversion.asInt(args[nParam++]):Tenant.ROOT_ID;
				
				List<SRepo> list = SolderVaultFactory.searchRepo(tenantId, repoIdPattern, schemaNamePattern);
				logConsole(String.format("Search for repo %s schema %s returned %d repos.",repoIdPattern,schemaNamePattern,list.size()));
				for (SRepo repo : list) {
					logConsole(String.format("\tRepo: %s",""+repo));
				}
			}
			break;
			
			
			case "delete":
			{
				initSolder("SolderCLIGitDelete",null,null);
				String repoId = args[nParam++];
				
				SRepo repo = SolderVaultFactory.getRepoById(repoId);
				logConsole(String.format("Found repo %s (sid=%d)",repo.getId(),repo.getSeqId()));
				repo.updateDelete();
				
				logConsole(String.format("Deleted; repo(postDelete)=%s",repo.toString()));
				
			}
			break;
			
			case "init": {
				File fileCache = makeFile("");
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				String repoId = args[nParam++];
				initSolder("SolderCLIGitInit",fileCache,repoId);
				gitClient.gitInit();
				break;
			}
			
			case "checkout": {
				File fileCache = makeFile("");
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				String repoId = null; //load it .srepo
				initSolder("SolderCLIGitCheckOut",fileCache,repoId);
				gitClient.gitCheckout();
				
				break;
			}
			
			case "push": {
				File fileCache = makeFile("");
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				String repoId = null; //load it .srepo
				initSolder("SolderCLIGitPush",fileCache,repoId);
				gitClient.gitPush();

				break;
			}
			
			
			case "status": {
				File fileCache = makeFile("");
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				String repoId = null; //load it .srepo
				initSolder("SolderCLIGitStatus",fileCache,repoId);
				gitClient.gitStatus();

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
	}
}