package org.solder.rest.solder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ee.rest.RestOp.RestClient;
import com.jnk.util.PrintUtils;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.lnk.lucene.TempFiles;

public class RestRepoFileService implements IRepoFileService {
	
	
	private static Log LOG = LogFactory.getLog(RestRepoFileService.class.getName());

	RestClient client;
	
	public RestRepoFileService(RestClient client) throws IOException{
		this.client = Objects.requireNonNull(client,"client");
		String stServerVersion = SolderRestClient.getVersion(client);
		LOG.info(String.format("Solder RepoFileService server verion= [%s]", stServerVersion));
	}
	
	public RestClient getRestClient() throws IOException {
		return client;
	}
	
	public SRepoInfo getRepo(String repoId) throws IOException {
		repoId = Validator.require(repoId, "repoId",Rules.TRIM_LOWER,Rules.NO_NULL_EMPTY);
		return SolderRestClient.getRepo(repoId, client);
	}
	
	public void refresh(SRepoInfo repoInfo) throws IOException {
		Objects.requireNonNull(repoInfo,"repo info");
		SRepoInfo repoRefresh = SolderRestClient.getRepo(repoInfo.getId(), client);
		repoInfo.refresh(repoRefresh);
	}
	
	public SCommitInfo getLatestCommit(SRepoInfo srepoInfo) throws IOException {
		//Got to
		Objects.requireNonNull(srepoInfo,"Repo Info");
		return SolderRestClient.getLatestCommit(srepoInfo.getId(), getRestClient());
	}
	
	
	public SCommitInfo getCommit(SRepoInfo repoInfo,int commitId) throws IOException {
		if (commitId<=0) {
			return getLatestCommit(repoInfo);
		} else {
			return SolderRestClient.getCommit(repoInfo.getId(),commitId,getRestClient());
		}
	}
	
	
	
	
	public File downloadFile(SRepoInfo srepoInfo,String relPath,long blobFsId,String stDigestExpect) throws IOException {
		Objects.requireNonNull(srepoInfo,"Repo Info");
		
		TempFiles tf = TempFiles.get(TempFiles.DEFAULT);
		File fileRoot = tf.getTempDir(srepoInfo.getId());
		
		File fileTmp = new File(fileRoot,""+blobFsId);
		Validator.checkNewFile(fileTmp,true, "New Temp file");
		
		

		OutputStream os = null;
		boolean fError =false;
		try {
			os = new FileOutputStream(fileTmp);
			fError =true;
			OutputStream osFinal = os;
			SolderRestClient.downloadFile(srepoInfo.getId(),relPath,blobFsId,stDigestExpect,()->osFinal,getRestClient());
			os.close();
			fError = false;
			return fileTmp;
		}finally {
			IOUtils.closeQuietly(os);
			if (fError) {
				tf.removeTempDir(fileRoot);
			}
		}
	}

	public CommitSession beginCommit(SCommitInfo commitInfoReq, List<String> listModEntryRelPath,
			List<String> listDelEntryRelPath) throws IOException {
		Objects.requireNonNull(commitInfoReq, "Commit Request");
		// Add contains both updated and new files. (Update can be found using the
		// previous commit, if needed)
		// Del only contain removed files

		return SolderRestClient.beginCommit(commitInfoReq, listModEntryRelPath.toArray(PrintUtils.EMPTY_STRING_ARRAY),
				listDelEntryRelPath.toArray(PrintUtils.EMPTY_STRING_ARRAY), getRestClient());
	}

	
	public long uploadFile(CommitSession cs,SolderEntry se) throws IOException {
	
		
		Objects.requireNonNull(cs,"commitSession");
		Objects.requireNonNull(se,"Solder Entry");

		File fileRep = se.getFile();
		Objects.requireNonNull(fileRep);
		Validator.checkFile(fileRep, "path " + se.getRelPath());
		InputStream is = null;
		try {
			is = new FileInputStream(fileRep);
			InputStream isFinal = is;
			long blobId = SolderRestClient.uploadFile(cs,se,()->isFinal, getRestClient());
			se.setBlobFsId(blobId);
			return blobId;
		} finally {
			IOUtils.closeQuietly(is);
		}
	}
	
	public SCommitInfo uploadCommit(CommitSession cs,File fileCommit) throws IOException {
		
		Objects.requireNonNull(cs,"commitSession");
		Validator.checkFile(fileCommit, "Commit File");
		String digest = SolderEntry.computeDigest(fileCommit);
		
		SCommitInfo scommit = SolderRestClient.uploadCommit(cs, fileCommit,digest, getRestClient());
		return Objects.requireNonNull(scommit,"scommit after upload");
	}
	
	

}
