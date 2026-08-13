package org.solder.rest.solder;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Used by Solder Clients.
 */
public  interface IRepoFileService {
	

	
	public SRepoInfo getRepo(String repoId) throws IOException;
	
	public void refresh(SRepoInfo repoInfo) throws IOException;
	
	public SCommitInfo getLatestCommit(SRepoInfo repoInfo) throws IOException;
	
	public SCommitInfo getCommit(SRepoInfo repoInfo,int commitId) throws IOException;
	
	
	public File downloadFile(SRepoInfo repoInfo,String relPath,long blobFsId,String stDigestExpect) throws IOException;
	
	
	
	public CommitSession beginCommit(SCommitInfo commitInfoReq,List<String> listAddEntryRelPath,List<String> listDelEntryRelPath) throws IOException;
	
	
	
	public long uploadFile(CommitSession cs,SolderEntry se) throws IOException;
	
	public SCommitInfo uploadCommit(CommitSession cs,File fileCommit) throws IOException;
	
}