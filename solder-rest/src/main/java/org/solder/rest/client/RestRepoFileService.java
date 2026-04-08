package org.solder.rest.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.rest.client.RemoteRepoSync.IRepoFileService;
import org.solder.rest.client.RemoteRepoSync.SolderEntry;

import com.ee.rest.RestOp.RestClient;
import com.jnk.util.TReference;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.lnk.lucene.TempFiles;

public class RestRepoFileService implements IRepoFileService {
	
	
	private static Log LOG = LogFactory.getLog(RestRepoFileService.class.getName());

	RestClient client;
	
	public RestRepoFileService(RestClient client) {
		this.client = Objects.requireNonNull(client,"client");
	}
	
	public RestClient getRestClient() throws IOException {
		return client;
	}
	
	public SRepoInfo getRepo(String repoId) throws IOException {
		repoId = Validator.require(repoId, "repoId",Rules.TRIM_LOWER,Rules.NO_NULL_EMPTY);
		return SolderRestClient.getRepo(repoId, client);
	}
	
	public SCommitInfo getLatestCommit(SRepoInfo srepoInfo) throws IOException {
		//Got to
		Objects.requireNonNull(srepoInfo,"Repo Info");
		return SolderRestClient.getLatestCommit(srepoInfo.getId(), getRestClient());
	}
	
	public File downloadFile(SRepoInfo srepoInfo,String relPath,long blobFsId) throws IOException {
		Objects.requireNonNull(srepoInfo,"Repo Info");
		
		TempFiles tf = TempFiles.get(TempFiles.DEFAULT);
		File fileRoot = tf.getTempDir(srepoInfo.getId());
		
		File fileTmp = new File(fileRoot,""+blobFsId);
		Validator.checkFile(fileTmp, "New Temp file");
		
		TReference<SCommitInfo> tref = new TReference<>();

		OutputStream os = null;
		boolean fError =false;
		try {
			os = new FileOutputStream(fileTmp);
			fError =true;
			OutputStream osFinal = os;
			SolderRestClient.downloadFile(srepoInfo.getId(),relPath,blobFsId,(sci)->tref.set(sci),()->osFinal,getRestClient());
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
	
	public int generateNewCommitId(SRepoInfo srepoInfo) throws IOException {
		Objects.requireNonNull(srepoInfo,"Repo Info");
		return SolderRestClient.getNewCommitId(srepoInfo.getId(), getRestClient());
	}
	
	public SCommitInfo createSCommit(SRepoInfo srepoInfo, String chash, Map<String, String> mapInfo, int commitId) throws IOException {
		Objects.requireNonNull(srepoInfo,"Repo Info");
		return new SCommitInfo(srepoInfo,chash,mapInfo,commitId);
	}
	
	
	public void uploadFile(SRepoInfo srepoInfo,SolderEntry se) throws IOException {
		
		Objects.requireNonNull(srepoInfo,"Repo Info");
		Objects.requireNonNull(se,"Solder Entry");

		File fileRep = se.getFile();
		Objects.requireNonNull(fileRep);
		Validator.checkFile(fileRep, "path " + se.getRelPath());
		InputStream is = null;
		try {
			is = new FileInputStream(fileRep);
			InputStream isFinal = is;
			long blobId = SolderRestClient.uploadFile(srepoInfo.getId(),se,()->isFinal, getRestClient());
			se.setBlobId(blobId);
		} finally {
			IOUtils.closeQuietly(is);
		}
	}
	
	public void commitUpload(SRepoInfo srepoInfo,SCommitInfo sci,File fileCommit,List<String> listDelEntryRelPath) throws IOException {
	
	
		
		Objects.requireNonNull(srepoInfo,"Repo Info");
		Objects.requireNonNull(sci,"SCommitInfo");
		InputStream is = null;
		try {
			String digest = RemoteRepoSync.computeDigest(fileCommit);
			is = new FileInputStream(fileCommit);
			
			InputStream isFinal = is;
			long blobFsId = SolderRestClient.commitUpload(srepoInfo.getId(), sci,digest,listDelEntryRelPath,()->isFinal, getRestClient());
			sci.setBlobFsId(blobFsId);

		} finally {
			IOUtils.closeQuietly(is);
		}
		
	
	}

}
