package org.solder.rest.client;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.rest.client.RemoteRepoSync.CommitInfo;
import org.solder.rest.client.RemoteRepoSync.IRepoFileService;
import org.solder.rest.client.RemoteRepoSync.SLocalRepo;

import com.jnk.util.PrintUtils;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;

/**
 * Used both by rest CLI, SolderCLI and also proxy
 */
public class SolderGitClient {
	
	static Log LOG = LogFactory.getLog(SolderGitClient.class.getName());
	
	IRepoFileService service;
	File fileCache;
	String repoId;
	
	Consumer<String> cConsole;
	
	void logConsole(String msg) {
		LOG.info(msg);
		if (cConsole!=null) {
			cConsole.accept(msg);
		}
	}
	
	public SolderGitClient(IRepoFileService service,File fileCache,String repoId,Consumer<String> cConsoleLogger) throws IOException {
		this.service=Objects.requireNonNull(service,"service");
		Validator.checkDir(fileCache, false, "Cache dir");
		this.fileCache=fileCache;
		if (StringUtils.isEmpty(repoId)) {
			repoId = RemoteRepoSync.readLocalRepoId(fileCache);
		}
		
		this.repoId = Validator.require(repoId, "repo id",Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
		this.cConsole = cConsoleLogger;
	}
	
	public void gitInit()  throws IOException {
		
		
		logConsole(String.format("Git Init Solder Rep %s using dir %s",repoId,fileCache.getAbsolutePath()));
		
		//Ensure the record is there..
		SRepoInfo repo = service.getRepo(repoId);
		
		SLocalRepo lrepo = new SLocalRepo(repo, fileCache,true);
		Objects.requireNonNull(service,"Repo File Service");
		int lrepoCommit = lrepo.getCommitId();
		int repoCommit = repo.getCommitId();
		
		if (repoCommit>0) {
			if (lrepoCommit==0) {
				// Repository has commits..
				// Get the Latest..
				logConsole(String.format("Repo %s Init to do autoCheckout prev=0; latest=%d  (date=%s)", repo.getId(), repoCommit,
					PrintUtils.print(repo.getCommitDate())));
				RemoteRepoSync.repoCheckout(lrepo,service);
			} else  {
				logConsole(String.format("Repo %s current state (commit=%d,hash=%s); Server commit Id=%d  (date=%s).",repo.getId(),lrepoCommit,lrepo.getCommitHash(), repoCommit,
						PrintUtils.print(repo.getCommitDate())));
			}
		} else {
			logConsole(String.format("Repo %s has no commits. Nothing to do", repo.getId()));
		}
	}
	
	
	public void gitStatus()  throws IOException {
		logConsole(String.format("Git Status Solder Rep %s using dir %s",repoId,fileCache.getAbsolutePath()));
		
		SRepoInfo repo = service.getRepo(repoId);
		
		SLocalRepo lrepo = new SLocalRepo(repo, fileCache,true);
		Objects.requireNonNull(service,"Repo File Service");
		int lrepoCommit = lrepo.getCommitId();
		int repoCommit = repo.getCommitId();
		
		logConsole(String.format("Repo %s current state (commit=%d,hash=%s); Server commit Id=%d  (date=%s).",repo.getId(),lrepoCommit,lrepo.getCommitHash(), repoCommit,
				PrintUtils.print(repo.getCommitDate())));
	}
	
	public void gitCheckout()  throws IOException {
		
		
		
		
		logConsole(String.format("Checout Solder Rep %s using dir %s",repoId,fileCache.getAbsolutePath()));
		
		SRepoInfo repo = service.getRepo(repoId);
		
		
		SLocalRepo lrepo = new SLocalRepo(repo, fileCache,false);
		RemoteRepoSync.repoCheckout(lrepo,service);
		logConsole(String.format("Done checkout"));
	}
	
	public void gitPush()  throws IOException {
		logConsole(String.format("Git Push Solder Rep %s using dir %s",repoId,fileCache.getAbsolutePath()));

		SRepoInfo repo = service.getRepo(repoId);
		
		SLocalRepo lrepo = new SLocalRepo(repo, fileCache,false);
		
		Objects.requireNonNull(service,"Repo File Service");
		int lrepoCommit = lrepo.getCommitId();
		int repoCommit = repo.getCommitId();
		
		//If LREPO and SREPO should have same commitId otherwise a new commit has commit has come , will require merge...
		if (lrepoCommit != repoCommit) {
			logConsole(String.format("Repo %s Repo commitId has Changed! current state (commit=%d,hash=%s); Server commit Id=%d  (date=%s). Requires Merge.",repo.getId(),lrepoCommit,lrepo.getCommitHash(), repoCommit,
					PrintUtils.print(repo.getCommitDate())));
			return;
		}
		
		
		CommitInfo commitInfo = RemoteRepoSync.repCommit(lrepo,fileCache,(mapCommit)->{
			mapCommit.put("cmsg", "SolderCLI gitpush");
		},service);
		if (!commitInfo.fNewCommit) {
			logConsole(String.format("repCommit %s found no new changes, Nothing to do prev commit=%d chash=%s",repo.getId(),commitInfo.getCommitId(),commitInfo.cHash));
			
		} else {
			logConsole(String.format("Git push %s new commit=%d, chash=%s",repo.getId(), commitInfo.getCommitId(),commitInfo.cHash));
		}
		logConsole(String.format("Done push"));
	}

}
