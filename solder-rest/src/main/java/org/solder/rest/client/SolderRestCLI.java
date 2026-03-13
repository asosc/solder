package org.solder.rest.client;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;

import org.apache.commons.cli.Option;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aura.crypto.CryptoScheme;
import com.ee.rest.RestException;
import com.ee.rest.RestOp;
import com.ee.rest.RestOp.ClientType;
import com.ee.rest.RestOp.RestClient;
import com.ee.rest.client.EnigmaRestClient;
import com.ee.rest.client.EnigmaRestOp;
import com.jnk.junit.AbstractCLI;
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
		SolderRestCLI cli = new SolderRestCLI();
		cli.runMain(a, true);
	}

	// ALL Handlers are here..

	
	static final String[] git_Ops = { "create","checkout","push","init","status","search","delete"};
	
	static final TreeMap<String,String> mapGitOpsHelp = new TreeMap<>();
	static {
		mapGitOpsHelp.put("create", 
				"Git create. Params: repoId schemaName [aoId]");
		
		mapGitOpsHelp.put("search", 
				"Search Repo. Params: [repoIdPattern schemaNamePattern]");
		mapGitOpsHelp.put("delete", 
				"Delete Repo (Marks for deletion). Params: repoId");
		
		mapGitOpsHelp.put("init",
				"Git init. Params:repoId");
		
		mapGitOpsHelp.put("checkout",
				"Git Checkout(same as clone,rebase). Params:");
		mapGitOpsHelp.put("push",
				"Git Push(same as commit and push). Params:");
		mapGitOpsHelp.put("status",
				"Git Status. Params:");
		
		
		
	
	}
	
	private static RestClient client =null;
	private static SolderGitClient gitClient = null;
	
	void initSolder(String stCmd,File fileCache,String repoId) throws IOException {
		
		
		
		//Need to get
		String tenant= "saltlick";
		String email = "stonecoal@saltlick";
		String user = tenant+"/"+email;
		String pwd = "p_"+email;
		
		
		
		client = new RestClient(ClientType.HTTP,"http://localhost:8080",RestOp.ENIGMA_SERVLET_URI);
		String sessKey = EnigmaRestClient.login(EnigmaRestOp.AUTH_TYPE_ENIGMA,user,pwd,client);
		LOG.info(String.format("Logged in; created session with key=%s",sessKey));
		
		
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
							
				int aoId = nParam>args.length?TypeConversion.asInt(args[nParam++]):Math.abs(r.nextInt());
				SRepoInfo srepoInfo =  SolderRestClient.createRepo(repoId,schemaName,aoId,client) ;
				logConsole("id: "+repoId+"; schema="+schemaName+"{; rsRepo="+srepoInfo);
				//gitCreate(fileCache,stId,schemaName,tenantId,aoId);
			}
			break;
			
			case "search":
			{
				
				initSolder("SolderCLIRepoSearch",null,null);
				
				String repoIdPattern = nParam<args.length?args[nParam++]:"";
				String schemaNamePattern =  nParam<args.length?args[nParam++]:"";
				
				SRepoInfo[] a =  SolderRestClient.searchRepo(repoIdPattern,schemaNamePattern,client) ;
				
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
				initSolder("SolderCLIGitInit",fileCache,repoId);
				
				gitClient.gitInit();
				
				break;
			}
			
			case "checkout": {
				File fileCache = makeFile("");
				logConsole("File Cache: "+fileCache.getAbsolutePath());
				Validator.checkDir(fileCache, false,"Git Cache");
				String repoId = null; //load it .srepo
				initSolder("SolderCLIGitCheckout",fileCache,repoId);
				
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
				throw new RestException("Unknown Op for git " + op);
			}
			}

		}

		public void close() {
			
		}
		
		
	
	
		

		

	}
	
}