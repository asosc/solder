package org.solder.vsync;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nimbo.blobs.BlobFS;
import org.nimbo.blobs.BlobFile;
import org.nimbo.blobs.BlobFileTransact;
import org.nimbo.blobs.Container;
import org.nimbo.blobs.ContainerGroup;
import org.solder.core.SolderException;
import org.solder.core.SolderMain;
import org.solder.rest.client.RemoteRepoSync;
import org.solder.rest.client.RemoteRepoSync.IRepoFileService;
import org.solder.rest.client.RemoteRepoSync.SolderEntry;
import org.solder.rest.client.SCommitInfo;
import org.solder.rest.client.SRepoInfo;
import org.solder.vsync.SolderVaultFactory.SCommit;
import org.solder.vsync.SolderVaultFactory.SRepo;

import com.aura.crypto.CryptoScheme;
import com.ee.rest.RestOp.RestClient;
import com.ee.session.SessionManager;
import com.jnk.util.CompareUtils;
import com.jnk.util.PrintUtils;
import com.jnk.util.Validator;

public class ServerRepoFileService  implements IRepoFileService {
	
	private static Log LOG = LogFactory.getLog(ServerRepoFileService.class.getName());
	
	static final ServerRepoFileService INSTANCE = new ServerRepoFileService();
	
	public static ServerRepoFileService get() {
		return INSTANCE;
	}
	
	
	private ServerRepoFileService() {}
	
	
	public RestClient getRestClient() throws IOException {
		throw new SolderException("Server side should not be asking for RestClient.!");
	}
	
	
	SRepo getSRepo(SRepoInfo srepoInfo) throws IOException {
		Objects.requireNonNull(srepoInfo,"srepo");
		return (SRepo)srepoInfo;
	}
	
	public SRepoInfo getRepo(String repoId) throws IOException {
		SRepo repo = SolderVaultFactory.getRepoById(repoId);
		return Objects.requireNonNull(repo,()->"repo "+repoId);
	}
	
	
	public SCommitInfo getLatestCommit(SRepoInfo srepoInfo) throws IOException {
		SRepo srepo = getSRepo(srepoInfo);
		return srepo.getLatestCommit();
	}
	
	public File downloadFile(SRepoInfo srepoInfo,String relPath,long blobFsId) throws IOException {
		SRepo srepo = getSRepo(srepoInfo);
		
		if (StringUtils.isEmpty(relPath)) {
			//Look for the root file.
			SCommit scommit = srepo.getLatestCommit();
			
				
			Objects.requireNonNull(scommit,"latest commit "+srepo.getId()+"; commitId="+srepo.getCommitId());
			if (blobFsId != scommit.getBlobFsId()) {
				throw new SolderException("Incorrect blobFsId expect:"+blobFsId+"; got "+scommit.getBlobFsId());
			}
			BlobFS blobFsCommit = scommit.getBlobFs();
			BlobFile blobFile = Container.read(blobFsCommit);
			return blobFile.getFile();
		} else {
			BlobFS blobFs = BlobFS.getById(blobFsId);
			Objects.requireNonNull(blobFs,"blobFs "+blobFsId);
			
			if (!blobFs.getOwnerApp().equals(SolderVaultFactory.TYPE) || !blobFs.getOwnerRef().equals(srepo.getId())) {
				LOG.error(String.format("Incorrect BlobFs call; id=%d; type=%s owner=%s expect=%s",blobFsId,blobFs.getOwnerApp(),blobFs.getOwnerRef(),srepo.getId()));
				throw new SolderException("BlobFsId mismatch for "+blobFsId);
			}
			BlobFile blobFile = Container.read(blobFs);
			return blobFile.getFile();
		}
	}
	
	public int generateNewCommitId(SRepoInfo srepoInfo) throws IOException {
		SRepo srepo = getSRepo(srepoInfo);
		return SolderVaultFactory.generateCommitId();
	}
	
	public SCommitInfo createSCommit(SRepoInfo srepoInfo, String chash, Map<String, String> mapInfo, int commitId) throws IOException {
		SRepo srepo = getSRepo(srepoInfo);
		return new SCommit(srepo,chash,mapInfo,commitId);
	}
	
	public void uploadFile(SRepoInfo srepoInfo,SolderEntry se) throws IOException {
		String cgName = SolderMain.getSolderContainerGroupName();
		ContainerGroup cg = ContainerGroup.get(cgName);
		
		SRepo srepo = getSRepo(srepoInfo);

		Objects.requireNonNull(cg, " cg " + cgName);
		MessageDigest md = RemoteRepoSync.tlMessageDigest.get();
		md.reset();
		CryptoScheme cs = CryptoScheme.getDefault();
		String name = cs.getUUID();

		Map<String, String> mapInfo = new HashMap<>();
		mapInfo.put("path", se.getRelPath());
		mapInfo.put("pid", SessionManager.getPid());

		File fileRep = se.getFile();
		//If fileRep is null, it means we are responding to an API call, the 
		//API caller must set the temp location...
		Objects.requireNonNull(fileRep);

		Validator.checkFile(fileRep, "path " + se.getRelPath());

		BlobFS blob = new BlobFS(name, SolderVaultFactory.TYPE, srepo.getId(), se.getCommitId(), mapInfo, srepo.getTenantId(),-1);
		BlobFileTransact bft = cg.beginFileTransact(blob);
		boolean fError = true;
		FileOutputStream fos = null;
		InputStream is = null;
		
		try {
			is = new FileInputStream(fileRep);
			File fileOut = bft.getFile();
			fos = new FileOutputStream(fileOut);
			DigestOutputStream dos = new DigestOutputStream(fos, md);
			IOUtils.copy(is, dos);
			dos.close();

			byte[] digest = md.digest();
			String digestNew = PrintUtils.toHexString(digest);
			blob.getInfo().put("digest", digestNew);
			if (!CompareUtils.stringEquals(digestNew, se.getDigest())) {
				String stError = String.format("Write digest mismatch for %s. writeDigest=%s, prevCalc=%s",
						se.getRelPath(), digestNew, se.getDigest());
				LOG.info(stError);
				throw new SolderException(stError);
			}
			fError = false;
			bft.commit();
			se.setBlobId(blob.getId());

		} finally {
			IOUtils.closeQuietly(fos, is);
			if (fError) {
				bft.abort();
			}
		}
	}
	
	
	
	public void commitUpload(SRepoInfo srepoInfo,SCommitInfo sci,File fileCommit,List<String> listDelEntryRelPath) throws IOException {
		
		String cgName = SolderMain.getSolderContainerGroupName();
		ContainerGroup cg = ContainerGroup.get(cgName);
		Objects.requireNonNull(cg, " cg " + cgName);
		
		
		SRepo srepo = getSRepo(srepoInfo);
		Objects.requireNonNull(sci, " sci ");
		Validator.checkFile(fileCommit, "fileCommit");
		
		
		SCommit scommit = (SCommit)sci;
		
		MessageDigest md = RemoteRepoSync.tlMessageDigest.get();
		md.reset();
		
		Map<String, String> mapInfo = new HashMap<>();
		mapInfo.put("pid", SessionManager.getPid());
		CryptoScheme cs = CryptoScheme.getDefault();
		String name = cs.getUUID();
		BlobFS blobCommit = new BlobFS(name, RemoteRepoSync.BLOB_TYPE_SOLDER_COMMIT, srepo.getId(), sci.getId(), mapInfo,
				srepo.getTenantId(), -1);
		BlobFileTransact bft = cg.beginFileTransact(blobCommit);
		boolean fError = true;
		FileOutputStream fos = null;
		InputStream is = null;
		try {
			is = new FileInputStream(fileCommit);
			File fileOut = bft.getFile();
			fos = new FileOutputStream(fileOut);
			DigestOutputStream dos = new DigestOutputStream(fos, md);
			IOUtils.copy(is, dos);
			dos.close();
			fError = false;
			bft.commit();
			// Create SCommit.
			scommit.setBlobFsId(blobCommit.getId());
			scommit.insert();
			srepo.updateCommit(scommit);

		} finally {
			IOUtils.closeQuietly(fos, is);
			if (fError) {
				bft.abort();
			}
		}
		
		if (listDelEntryRelPath!=null && listDelEntryRelPath.size() > 0) {
			List<BlobFS> listPrev = BlobFS.selectByOwner(SolderVaultFactory.TYPE, srepo.getId());
			LOG.info(String.format("SVault rep %s found %d objects.", srepo.getId(), listPrev.size()));

			Map<String, BlobFS> mapBlobFSPrev = new LinkedHashMap<>();
			for (BlobFS blobFS : listPrev) {
				String stRelPath = blobFS.getInfo().get("path");
				if (stRelPath != null) {
					mapBlobFSPrev.put(stRelPath, blobFS);
				}
			}
			String ownerDelete = srepo.getId() + "_del";
			for (String stDelRelPath : listDelEntryRelPath) {
				BlobFS fsToDelete = mapBlobFSPrev.get(stDelRelPath);
				if (fsToDelete != null) {
					fsToDelete.updateOwner(null, ownerDelete);
				}
			}
		}
	}
		
}




