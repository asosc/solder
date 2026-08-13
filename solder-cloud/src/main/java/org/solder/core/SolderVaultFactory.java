package org.solder.core;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.rest.solder.IRepoFileService;
import org.solder.rest.solder.RemoteRepoSync;
import org.solder.rest.solder.SLocalRepo;
import org.solder.vsync.SyncLocalRepo;

import com.beech.store.FileVaultProvider;
import com.beech.store.IVaultFactory;
import com.beech.store.IVaultProvider;
import com.beech.store.TVault;
import com.ee.session.db.Event;
import com.jnk.util.PrintUtils;

public class SolderVaultFactory implements IVaultFactory {

	private static Log LOG = LogFactory.getLog(SolderVaultFactory.class.getName());

	public static final String TYPE = "solder_repo";

	Map<String,SolderVaultProvider> mapProv = new LinkedHashMap<>();
	public SolderVaultFactory() {
		mapProv = new LinkedHashMap<>();
		TVault.registerFactory(this);
	}

	public String getType() {
		return TYPE;
	}

	public synchronized IVaultProvider getProvider(String stFactoryParams, boolean fReadOnly) throws IOException {
		String id = stFactoryParams;
		SolderVaultProvider prov = mapProv.get(id);
		if (prov != null) {
			return prov;
		}
		
		//Vault Provider only deals with latest version of repo.
		//Use other tools if other version is needed
		
		SRepo srepo = SRepo.getRepoById(id);
		Objects.requireNonNull(srepo, "srepo " + id);
		prov = new SolderVaultProvider(srepo,null,false);
		mapProv.put(id, prov);
		return prov;
	}
	
	public void repoGitPush(String stFactoryParams) throws IOException {
		SolderVaultProvider prov = (SolderVaultProvider)getProvider(stFactoryParams,false);
		prov.repoGitPush();
	}
	
	
	static class SolderVaultProvider implements IVaultProvider {
		
		
		SyncLocalRepo syncCache;
		File fileProv;
		SLocalRepo lrepo;
		FileVaultProvider fvp;
		SRepo repo;
		SCommit scommit;
		boolean fReadOnly;
		
		SolderVaultProvider(SRepo repo,SCommit scommitOpen,boolean fReadOnly) throws IOException{
			this.repo=repo;
			this.fReadOnly = fReadOnly;
			syncCache = SyncLocalRepo.get(SyncLocalRepo.DEFAULT);
			fileProv = syncCache.ensureSyncFolder(repo.getId());
			//Solder Sync On...
			lrepo = new SLocalRepo(SRepo.makeSRepoInfo(repo), fileProv,true);
			//We want to make sure the lrepo has 
			
			
			// If we opened in readonly more or there are no repository to begin with
			//everything works as it is now.
			//If the commit ids are different and our local repo contains no uncommitted 
			//write, this works too.
			//If there is a change (2 kinds, one where we did not commit to the TOC, that too works
			//If a commit to TOC has occured and the server has never seen that commit,
			//We have a decision to make. - Either it was  a local change that was never meant to be
			//pushed or we made local changes and the push never occured  for whatever reason (by design or error).
			
			//Apps should try to use a single process to update as it would minimize loss of data but it is like any
			//commit, if you manage to commit you have it or else you dont. In this, apps will expects server sync
			//when local commit was successful. Apps should make sure it pushes its changes immediately or should have logic
			//to protect itself from loss of commits.
			
			//This case is an issue ever if a write is only done by a single process (a restart could cause this miss).
			if (scommitOpen!=null) {
				repo.verifyCommit(scommitOpen);
				this.scommit=scommitOpen;
			} else {
				this.scommit = repo.getLatestCommit();
			}
			
			String stScommitHash = scommit!=null?scommit.getCHash():"";
			int scommitId = scommit!=null?scommit.getId():-1;
			
			
			if (repo.getCommitId()>0 && lrepo.getCommitId() != repo.getCommitId() ) {
				//Need to Sync...
			
				Event.log(SEvent.SRepoSync, repo.getSeqId(), repo.getTenantId(), (mb) -> {
					mb.put("fn", "svp.new_checkout");
					mb.put("ldir", fileProv.getAbsolutePath());
					mb.put("id", repo.getId());
					mb.put("lrepoCommitId", lrepo.getCommitId());
					mb.put("lrepoCommitHash", lrepo.getCommitHash());
					mb.put("commitId", repo.getCommitId());
					mb.put("scommitId", scommitId);
					mb.put("scommitHash", stScommitHash);
				});
				
				IRepoFileService rfs = ServerRepoFileService.get();
				RemoteRepoSync.repoCheckout(lrepo,SCommit.makeSCommitInfo(this.scommit),rfs);
			} else {
				Event.log(SEvent.SRepoSync, repo.getSeqId(), repo.getTenantId(), (mb) -> {
					mb.put("fn", "svp.new_reuse");
					mb.put("ldir", fileProv.getAbsolutePath());
					mb.put("id", repo.getId());
					mb.put("lrepoCommitId", lrepo.getCommitId());
					mb.put("lrepoCommitHash", lrepo.getCommitHash());
					mb.put("commitId", repo.getCommitId());
					mb.put("scommitId", scommitId);
					mb.put("scommitHash", stScommitHash);
				});
			}
			fvp = new FileVaultProvider(fileProv.getAbsolutePath(), fReadOnly);
		}
		
		public void repoGitPush() throws IOException{
			IRepoFileService rfs = ServerRepoFileService.get();
			
			try {
				RemoteRepoSync.repCommit(lrepo, fileProv, (props)->{
					props.put("message","SolderVaultProvider");
				},rfs);
				SCommit scommit = repo.scommit;
				String stScommitHash = scommit!=null?scommit.getCHash():"";
				int scommitId = scommit!=null?scommit.getId():-1;
				Event.log(SEvent.SRepoSync, repo.getSeqId(), repo.getTenantId(), (mb) -> {
					mb.put("fn", "svp.repoGitPush");
					mb.put("ldir", fileProv.getAbsolutePath());
					mb.put("id", repo.getId());
					mb.put("lrepoCommitId", lrepo.getCommitId());
					mb.put("lrepoCommitHash", lrepo.getCommitHash());
					mb.put("commitId", repo.getCommitId());
					mb.put("scommitId", scommitId);
					mb.put("scommitHash", stScommitHash);
				});
			}catch(Exception e) {
				
				SCommit scommit = repo.scommit;
				String stScommitHash = scommit!=null?scommit.getCHash():"";
				int scommitId = scommit!=null?scommit.getId():-1;
				
				Event.log(SEvent.SRepoSyncError, repo.getSeqId(), repo.getTenantId(), (mb) -> {
					mb.put("fn", "svp.repoGitPush");
					mb.put("ldir", fileProv.getAbsolutePath());
					mb.put("id", repo.getId());
					mb.put("lrepoCommitId", lrepo.getCommitId());
					mb.put("lrepoCommitHash", lrepo.getCommitHash());
					mb.put("commitId", repo.getCommitId());
					mb.put("scommitId", scommitId);
					mb.put("scommitHash", stScommitHash);
					mb.put("error", PrintUtils.getStackTrace(e));
				});
				
				throw SolderException.rethrow(e);
			}
		}
		
		
		public String getName() {
			return repo.getId();
		}
		
		public String getFactoryParam() {
			return repo.getId();
		}
		
		public String getType() {
			return TYPE;
		}
		
		public String[] getRoots() throws IOException {
			return fvp.getRoots();
		}
		
		public void ensureRoot(String name) throws IOException {
			fvp.ensureRoot(name);
		}
		
		
		public boolean isReadOnly() {
			return fReadOnly;
		}
		
		
		
		//All files will start with one of the /rootName/...
		public File getExistingFile(String path,boolean fThrow) throws IOException {
			return fvp.getExistingFile(path, fThrow);
		}
		
		//Will throw if there is an existing file.. (Apps can use getExistingFile and deleteFile if it needs to clean up)
		public File getNewFile(String path) throws IOException {
			return fvp.getNewFile(path);
		}
		
		public boolean deleteFile(String path) throws IOException {
			return fvp.deleteFile(path);
		}
		
		public List<String> listFile(String path) throws IOException {
			return fvp.listFile(path);
		}
		
	}


}
