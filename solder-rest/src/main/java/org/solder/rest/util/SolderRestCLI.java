package org.solder.rest.util;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.commons.cli.Option;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.rest.solder.RestRepoFileService;
import org.solder.rest.solder.SRepoInfo;
import org.solder.rest.solder.SUsageEntry;
import org.solder.rest.solder.SolderGitClient;
import org.solder.rest.solder.SolderRestClient;

import com.aura.crypto.CryptoScheme;
import com.ee.rest.RestException;
import com.ee.rest.RestOp.RestClient;
import com.ee.rest.client.ECred;
import com.jnk.junit.AbstractCLI;
import com.jnk.util.TParseUtil;
import com.jnk.util.TypeConversion;
import com.jnk.util.Validator;
import com.jnk.util.random.IRandom;
import com.lnk.lucene.TempFiles;

public class SolderRestCLI  extends AbstractCLI {

	static Log LOG = LogFactory.getLog(SolderRestCLI.class.getName());

	public SolderRestCLI() {
		super("SolderRestCLI");

		// Op incluse [list,extract]

		options.addOption(
				new Option("g", "git", true, String.format("Git: op opParams\r\n%s", Arrays.toString(git_Ops))));
		
		options.addOption(
				new Option("c", "cred", true, String.format("Cred File")));
		
		
	

	}

	void init() throws IOException {
		// Initialize all things.
		
		TempFiles.initDefault();
	}
	
	File fileCred = null;

	public boolean doRunMain() throws Exception {

		init();
		
		

		String[] args = cline.getArgs();
		int nParam = 0;
		
		if (cline .hasOption("cred")) {
			System.out.println("Git command");
			fileCred = new File(cline.getOptionValue("cred"));
			Validator.checkFile(fileCred,"Cred file");
		}

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
		System.out.println("**********Git Ops************\r\n");
		for (var entry : mapGitOpsHelp.entrySet()) {
			System.out.println(String.format("%s ->%s", entry.getKey(),entry.getValue()));
		}
		System.out.println("**********End Git Ops************\r\n");
	}

	public static void main(String[] a) {
		SolderRestCLI cli = new SolderRestCLI();
		cli.runMain(a, true);
	}

	// ALL Handlers are here..

	
	static final String[] git_Ops = { "create","checkout","push","init","status","search","delete","prune","orphan","usagereport","purge"};
	
	static final TreeMap<String,String> mapGitOpsHelp = new TreeMap<>();
	static {
		mapGitOpsHelp.put("create", 
				"Git create. Params: repoId schemaName [aoId,tag]");
		
		mapGitOpsHelp.put("search", 
				"Search Repo. Params: [repoIdPattern schemaNamePattern, tagFilter]");
		mapGitOpsHelp.put("delete", 
				"Delete Repo (Marks for deletion). Params: repoId");
		
		mapGitOpsHelp.put("init",
				"Git init. Params:repoId [commitId]");
		
		mapGitOpsHelp.put("checkout",
				"Git Checkout(same as clone,rebase). Params: [commitId]");
		mapGitOpsHelp.put("push",
				"Git Push(same as commit and push). Params:");
		mapGitOpsHelp.put("status",
				"Git Status. Params:");
		mapGitOpsHelp.put("prune",
				"Prune commits (throw away older storage snaps). Params: repoId commitCsvToKeep [fDryRun]. Latest tip is always kept. After prune, run orphan to reclaim blob space.");
		mapGitOpsHelp.put("orphan",
				"Remove orphan files from repo. Params:repoId [fDryRun]. Blocked if tip or tip's prev commit failed to scan.");
		mapGitOpsHelp.put("usagereport",
				"Generates usage report for the repo (via REST). Params:repoId outDir");
		mapGitOpsHelp.put("purge",
				"Purges an already deleted repo. Params:repoId [fDryRun]");
	}
	
	private static RestClient client =null;
	private static SolderGitClient gitClient = null;
	
	void initSolder(String stCmd,File fileCache,String repoId) throws IOException {
		
		
		File fileEc = this.fileCred;
		if (fileEc == null) {
			//Use the one pytest (if available..
			Map<String, String> mapEnv = System.getenv();
			String stProxyInstallRoot = mapEnv.get("BPROXY_INSTALL");
			File filePyConfig = new File(stProxyInstallRoot,"pyconfig");
			Validator.checkDir(filePyConfig, false, "pyConfig Config Root");
			fileEc = new File(filePyConfig,"ec.cfg");
			Validator.checkFile(fileEc,"Cred file");
		}
		
		logConsole(String.format("Using cred file %s", fileEc.getAbsolutePath()));
		client = ECred.getRestClient(fileEc);
			
		Objects.requireNonNull(client);
		
		if (fileCache != null) {
			RestRepoFileService service = new RestRepoFileService(client);
			gitClient = new SolderGitClient(service,fileCache,repoId,(st)->logConsole(st));
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
				
				
				
				
				IRandom r = CryptoScheme.getDefault().getRandom();
				String repoId = args[nParam++];
				String schemaName = args[nParam++];
				String tag = nParam<args.length?args[nParam++]:null;
							
				int aoId = nParam>args.length?TypeConversion.asInt(args[nParam++]):Math.abs(r.nextInt());
				SRepoInfo srepoInfo =  SolderRestClient.createRepo(repoId,schemaName,aoId,tag,client) ;
				logConsole("id: "+repoId+"; schema="+schemaName+"{; rsRepo="+srepoInfo);
				//gitCreate(fileCache,stId,schemaName,tenantId,aoId);
			}
			break;
			
			case "search":
			{
				
				initSolder("SolderCLIRepoSearch",null,null);
				
				String repoIdPattern = nParam<args.length?args[nParam++]:"";
				String schemaNamePattern =  nParam<args.length?args[nParam++]:"";
				String tagFilter =  nParam<args.length?args[nParam++]:null;
				if (tagFilter != null && tagFilter.isBlank()) {
					tagFilter = null;
				}
				
				SRepoInfo[] a =  SolderRestClient.searchRepo(repoIdPattern,schemaNamePattern,tagFilter,client) ;
				
				logConsole(String.format("Search for repo %s schema %s returned %d repos.",repoIdPattern,schemaNamePattern,a.length));
				for (SRepoInfo repo : a) {
					logConsole(String.format("\tRepo: %s",""+repo));
				}
				//gitCreate(fileCache,stId,schemaName,tenantId,aoId);
			}
			break;
			
			case "delete":
			{
				
				initSolder("SolderCLIRepoDelete",null,null);
				
				String repoId = args[nParam++];
				
				SRepoInfo repo = SolderRestClient.deleteRepo(repoId, client);
				logConsole(String.format("Deleted; repo(postDelete)=%s",repo.toString()));
				//gitCreate(fileCache,stId,schemaName,tenantId,aoId);
			}
			break;
			
			case "init": {
				
				File fileCache = makeFile("");
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				String repoId = args[nParam++];
				int commitId = nParam<args.length?TypeConversion.asInt(args[nParam++]):-1;
				
				File fileRepo = new File(fileCache,repoId);
				Validator.checkDir(fileRepo, true,"Git Repo Cache");
				initSolder("SolderCLIGitInit",fileRepo,repoId);
				
				gitClient.gitInit(commitId);
				
				break;
			}
			
			case "checkout": {
				File fileCache = makeFile("");
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				String repoId = null; //load it .srepo
				initSolder("SolderCLIGitCheckout",fileCache,repoId);
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
				initSolder("SolderRestCLIPrune", null, null);
				String repoId = args[nParam++];
				int[] aCommitIdsToKeep = TParseUtil.parseIntCsv(args[nParam++]);
				boolean fDryRun = nParam < args.length ? TypeConversion.asBoolean(args[nParam++]) : true;
				logConsole(String.format("Prune repo %s toKeep=%s fDryRun=%s", repoId,
						Arrays.toString(aCommitIdsToKeep), Boolean.toString(fDryRun)));
				int[] aRemoved = SolderRestClient.pruneCommits(repoId, aCommitIdsToKeep, fDryRun, client);
				logConsole(String.format("Prune %s removed %d commits: %s",
						fDryRun ? "dry-run would remove" : "removed", aRemoved.length, Arrays.toString(aRemoved)));
				break;
			}

			case "orphan": {
				initSolder("SolderRestCLIOrphan", null, null);
				String repoId = args[nParam++];
				boolean fDryRun = nParam < args.length ? TypeConversion.asBoolean(args[nParam++]) : true;
				logConsole(String.format("Remove Orphan from repo %s [fDryRun=%s]", repoId, Boolean.toString(fDryRun)));
				SUsageEntry[] a = SolderRestClient.removeOrphans(repoId, fDryRun, client);
				logConsole(String.format("Orphan %s %d blobs:", fDryRun ? "would delete" : "deleted", a.length));
				for (SUsageEntry ue : a) {
					logConsole(String.format("\t%s sid=%d commitId=%d blobFsId=%d sz=%d charged=%s",
							ue.getRelPath(), ue.getSid(), ue.getCommitId(), ue.getBlobFsId(), ue.size(),
							Boolean.toString(ue.isCharged())));
				}
				break;
			}

			case "usagereport": {
				initSolder("SolderRestCLIUsageReport", null, null);
				String repoId = args[nParam++];
				File outDir = new File(args[nParam++]);
				outDir = outDir.getCanonicalFile();
				Validator.checkDir(outDir, true, "Usage outDir");

				logConsole(String.format("Repo %s Usage; outDir=%s", repoId, outDir.getAbsolutePath()));
				SUsageEntry[] a = SolderRestClient.getAllUsage(repoId, false, client);
				long szCharged = 0L;
				long szOrphan = 0L;
				File fileOut = new File(outDir, repoId + "_usage.csv");
				Validator.checkNewFile(fileOut, true, "Usage out file");
				try (FileWriter w = new FileWriter(fileOut, StandardCharsets.UTF_8)) {
					w.write("sid,commitId,type,path,blobFsId,blobFound,sz,charged\r\n");
					for (SUsageEntry ue : a) {
						long sz = Math.max(0L, ue.size());
						if (ue.isCharged()) {
							szCharged += sz;
							if (ue.getType() == SUsageEntry.SueType.ORPHAN) {
								szOrphan += sz;
							}
						}
						w.write(String.format("%d,%d,%s,%s,%d,%s,%d,%s\r\n", ue.getSid(), ue.getCommitId(),
								ue.getType().name(), ue.getRelPath(), ue.getBlobFsId(),
								Boolean.toString(ue.isBlobFSFound()), ue.size(), Boolean.toString(ue.isCharged())));
						logConsole(String.format("\t%s type=%s blobFsId=%d sz=%d charged=%s", ue.getRelPath(),
								ue.getType().name(), ue.getBlobFsId(), ue.size(), Boolean.toString(ue.isCharged())));
					}
				}
				logConsole(String.format("Repo %s uses %,d bytes and has orphan %,d bytes. Wrote %s", repoId, szCharged,
						szOrphan, fileOut.getAbsolutePath()));
				break;
			}

			case "purge": {
				initSolder("SolderRestCLIPurge", null, null);
				String repoId = args[nParam++];
				boolean fDryRun = nParam < args.length ? TypeConversion.asBoolean(args[nParam++]) : true;
				logConsole(String.format("Purge repo %s fDryRun=%s", repoId, Boolean.toString(fDryRun)));
				SolderRestClient.purge(repoId, fDryRun, client);
				logConsole(String.format("Purge %s for repo %s", fDryRun ? "dry-run completed" : "completed", repoId));
				break;
			}
			
			default: {
				logConsole("Unknown Op for git " + op);
				throw new RestException("Unknown Op for git " + op);
			}
			}

		}

		public void close() {
			
		}
		
		
	
	
		

		

	}
	
}