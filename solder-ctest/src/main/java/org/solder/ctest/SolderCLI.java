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
import org.solder.core.SRepo;
import org.solder.core.SRepoUtil;
import org.solder.core.SRepoUtil.SRepoUsage;
import org.solder.core.SRepoUtil.UsageEntry;
import org.solder.core.ServerRepoFileService;
import org.solder.core.SolderException;
import org.solder.core.SolderMain;
import org.solder.rest.solder.SUsageEntry;
import org.solder.rest.solder.SolderGitClient;

import com.aura.crypto.CryptoScheme;
import com.ee.rest.RestException;
import com.ee.session.ISession;
import com.ee.session.SessionManager;
import com.ee.session.db.EESessionProvider;
import com.ee.session.db.Tenant;
import com.ee.util.Config;
import com.jnk.junit.AbstractCLI;
import com.jnk.util.TParseUtil;
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
	
	public void printHelp() throws IOException{
		super.printHelp();
		System.out.println("**********Op Details************\r\n");
		System.out.println("Note: repo lookup accepts string id and/or numeric sid. If sid is omitted/<=0 and");
		System.out.println("      repoId is all digits, it is also tried as sid first, then as string id.");
		System.out.println("      Prefer non-numeric repo ids so they cannot collide with a seq id (sid).");
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

	static final String[] git_Ops = { "create","checkout","push","init","status","search","delete","prune","orphan","usagereport","purge"};
	static final TreeMap<String,String> mapGitOpsHelp = new TreeMap<>();
	static {
		mapGitOpsHelp.put("create", 
				"Git create. Params: fileLocalRepo repoId schemaName [aoId tag tenant_id]");
		mapGitOpsHelp.put("search", 
				"Search Repo. Params: repoIdPattern schemaNamePattern tagFilter [tenant_id]");
		mapGitOpsHelp.put("delete", 
				"Delete Repo. Params: repoId (string id; numeric also tried as sid — prefer non-numeric ids)");
		
		mapGitOpsHelp.put("init",
				"Git init. Params:repoId (string id; numeric also tried as sid — prefer non-numeric ids)");
		
		mapGitOpsHelp.put("checkout",
				"Git Checkout(same as clone,rebase). Params:");
		mapGitOpsHelp.put("push",
				"Git Push(same as commit and push). Params:");
		mapGitOpsHelp.put("status",
				"Git Status. Params:");
		
		mapGitOpsHelp.put("prune",
				"Prune commits (throw away older storage snaps). Params: repoId commitCsvToKeep [fDryRun]. Latest tip is always kept. After prune, run orphan to reclaim blob space. repoId: numeric also tried as sid.");
		
		
		
		mapGitOpsHelp.put("orphan",
				"Remove orphan files from repo. Params:repoId [fDryRun]. Blocked if tip or tip's prev commit failed to scan. repoId: numeric also tried as sid.");
		mapGitOpsHelp.put("usagereport",
				"Generates usage report for the  repo. Params:repoId outDir. repoId: numeric also tried as sid.");
		mapGitOpsHelp.put("purge",
				"Purges an already deleted repo. Params:repoId [fDryRun]. repoId: numeric also tried as sid.");
	}
	
	private static SolderGitClient gitClient = null;
	void initSolder(String stCmd,File fileCache,String repoId) throws IOException {
		
		Map<String, String> mapEnv = System.getenv();
		String stInstall = mapEnv.get("ENIGMA_INSTALL");
		File fileInstall = makeFile(stInstall);
		File fileEECfg = new File(fileInstall,"ens/WEB-INF/ens.cfg");
		logConsole("Initializing EESessionProvider using config "+fileEECfg.getAbsolutePath());
		Validator.checkFile(fileEECfg, "Given config file");
		
		Config cfg =EESessionProvider.init(fileEECfg);
		logConsole("Success initializing EESessionProvider using config "+fileEECfg.getAbsolutePath());
		ISession s = SessionManager.createSystemSession();
		s.beginTrans("InitSolder", null, false);
		SolderMain.init(cfg);
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
				String tSchema = args[nParam++];
				
				int tenantId = nParam<args.length?TypeConversion.asInt(args[nParam++]):Tenant.ROOT_ID;
				int aoId = nParam<args.length?TypeConversion.asInt(args[nParam++]):Math.abs(r.nextInt());
				String tag = nParam<args.length?args[nParam++]:null;
				
				logConsole("id: "+repoId+"; schema="+tSchema);
				
				SRepo repo = SRepo.ensureSRepo(repoId,tSchema,tenantId, aoId,tag);
				
				logConsole(String.format("EnsureSRepo  %s schemaName=%s, tenantId=%d, aoId=%d, tag=%s",repo.getId(),repo.getTSchema(),repo.getTenantId(),repo.getAoId(),repo.getTag()));
				
			}
			break;
			
			case "search":
			{
				initSolder("SolderCLIGitSearch",null,null);
				
				String repoIdPattern = nParam<args.length?args[nParam++]:"";
				String tSchemaPattern =  nParam<args.length?args[nParam++]:"";
				String tagFilter =  nParam<args.length?args[nParam++]:"";
				int tenantId = nParam<args.length?TypeConversion.asInt(args[nParam++]):Tenant.ROOT_ID;
				
				List<SRepo> list = SRepo.searchRepo(tenantId, repoIdPattern, tSchemaPattern,tagFilter);
				logConsole(String.format("Search for repo %s schema %s returned %d repos.",repoIdPattern,tSchemaPattern,list.size()));
				for (SRepo repo : list) {
					logConsole(String.format("\tRepo: %s",""+repo));
				}
			}
			break;
			
			
			case "delete":
			{
				initSolder("SolderCLIGitDelete",null,null);
				String repoId = args[nParam++];
				
				SRepo repo = SRepo.getRepoById(repoId);
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
				int commitId = nParam<args.length?TypeConversion.asInt(args[nParam++]):-1;
				initSolder("SolderCLIGitInit",fileCache,repoId);
				gitClient.gitInit(commitId);
				break;
			}
			
			case "checkout": {
				File fileCache = makeFile("");
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				String repoId = null; //load it .srepo
				initSolder("SolderCLIGitCheckOut",fileCache,repoId);
				int commitId = nParam<args.length?TypeConversion.asInt(args[nParam++]):-1;
				gitClient.gitCheckout(commitId);
				
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
			
			
			case "prune": {
				initSolder("SolderCLIPrune",null,null);
				String repoId = args[nParam++];
				int[] aCommitIdsToKeep= TParseUtil.parseIntCsv(args[nParam++]);
				boolean fDryRun = nParam<args.length?TypeConversion.asBoolean(args[nParam++]):true;
				logConsole(String.format("Prune repo %s toKeep=%s fDryRun=%s",repoId,Arrays.toString(aCommitIdsToKeep),Boolean.toString(fDryRun)));
				SRepo repo = SRepo.getRepoById(repoId);
				logConsole(String.format("Found repo %s (sid=%d)",repo.getId(),repo.getSeqId()));
				int[] aRemoved = repo.pruneCommits(aCommitIdsToKeep, fDryRun);
				logConsole(String.format("Prune %s removed %d commits: %s",
						fDryRun ? "dry-run would remove" : "removed", aRemoved.length, Arrays.toString(aRemoved)));
				break;
			}
				
			case "orphan": {
				initSolder("SolderCLIOrphan",null,null);
				String repoId = args[nParam++];
				boolean fDryRun = nParam<args.length?TypeConversion.asBoolean(args[nParam++]):true;
				logConsole(String.format("Remove Orphan from repo %s [fDryRun=%s]",repoId,Boolean.toString(fDryRun)));
				SRepo repo = SRepo.getRepoById(repoId);
				logConsole(String.format("Found repo %s (sid=%d)",repo.getId(),repo.getSeqId()));
				SRepoUsage sru = new SRepoUsage(repo);
				List<UsageEntry> list = sru.doRemoveOrphan(fDryRun);
				logConsole(String.format("Orphan %s %d blobs:", fDryRun ? "would delete" : "deleted", list.size()));
				for (UsageEntry ue : list) {
					SUsageEntry sue = SRepoUtil.makeSUsageEntry(ue);
					logConsole(String.format("\t%s sid=%d commitId=%d blobFsId=%d sz=%d charged=%s",
							sue.getRelPath(), sue.getSid(), sue.getCommitId(), sue.getBlobFsId(), sue.size(),
							Boolean.toString(sue.isCharged())));
				}
				break;
			}
			
			
			case "usagereport": {
				initSolder("SolderCLIUsageReport",null,null);
				String repoId = args[nParam++];
				File outDir = new File(args[nParam++]);
				outDir = outDir.getCanonicalFile();
				Validator.checkDir(outDir,true,"Usage outDir");
				
				logConsole(String.format("Repo %s Usage; outDir=%s",repoId,outDir.getAbsolutePath()));
				SRepo repo = SRepo.getRepoById(repoId);
				logConsole(String.format("Found repo %s (sid=%d)",repo.getId(),repo.getSeqId()));
				SRepoUsage sru = new SRepoUsage(repo);
				List<UsageEntry> list = sru.getAllEntry();
				long szCharged = 0L;
				long szOrphan = 0L;
				for (UsageEntry ue : list) {
					SUsageEntry sue = SRepoUtil.makeSUsageEntry(ue);
					long sz = Math.max(0L, sue.size());
					if (sue.isCharged()) {
						szCharged += sz;
						if (sue.getType() == SUsageEntry.SueType.ORPHAN) {
							szOrphan += sz;
						}
					}
					logConsole(String.format("\t%s type=%s blobFsId=%d sz=%d charged=%s", sue.getRelPath(),
							sue.getType().name(), sue.getBlobFsId(), sue.size(), Boolean.toString(sue.isCharged())));
				}
				sru.getUsageCsv(outDir);
				File fileOut = new File(outDir, repo.getSeqId() + "_usage.csv");
				logConsole(String.format("Repo %s(%d) uses %,d bytes and has orphan %,d bytes. Wrote %s",
						repo.getId(), repo.getSeqId(), szCharged, szOrphan, fileOut.getAbsolutePath()));
				break;
			}
			
			case "purge": {
				initSolder("SolderCLIPurge",null,null);
				String repoId = args[nParam++];
				boolean fDryRun = nParam<args.length?TypeConversion.asBoolean(args[nParam++]):true;
				
				SRepo repo = SRepo.getRepoById(repoId);
				
				logConsole(String.format("Purge repo %s fDryRun=%s", repoId, Boolean.toString(fDryRun)));
				logConsole(String.format("Found repo %s (sid=%d) fDeleted=%s, fDryRun=%s",repo.getId(),repo.getSeqId(),Boolean.toString(repo.isDeleted()),Boolean.toString(fDryRun)));
				if (!repo.isDeleted()) {
					throw new RestException("Repo cannot be purged. Need to be marked for deletion!");
				}
				SRepoUsage sru = new SRepoUsage(repo);
				sru.purgeRepo(fDryRun);
				logConsole(String.format("Purge %s for repo %s", fDryRun ? "dry-run completed" : "completed", repoId));
				
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