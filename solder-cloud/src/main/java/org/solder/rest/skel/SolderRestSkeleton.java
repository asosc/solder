package org.solder.rest.skel;



import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.core.SolderException;
import org.solder.rest.client.RemoteRepoSync;
import org.solder.rest.client.RemoteRepoSync.IRepoFileService;
import org.solder.rest.client.RemoteRepoSync.SolderEntry;
import org.solder.rest.client.SCommitInfo;
import org.solder.rest.client.SolderRestOp;
import org.solder.vsync.ServerRepoFileService;
import org.solder.vsync.SolderSentryProvider;
import org.solder.vsync.SolderVaultFactory;
import org.solder.vsync.SolderVaultFactory.SCommit;
import org.solder.vsync.SolderVaultFactory.SRepo;

import com.ee.ens.EnServlet;
import com.ee.ens.EnServlet.SCall;
import com.ee.rest.RestException;
import com.ee.rest.RestOp;
import com.ee.rest.RestProcessor;
import com.ee.rest.RestSkeletonState;
import com.ee.session.SessionManager;
import com.ee.session.db.EEvent;
import com.ee.session.db.Event;
import com.ee.session.db.RolePriv;
import com.ee.session.db.RolePriv.Scope;
import com.ee.session.db.SentryProvider;
import com.ee.session.db.User;
import com.jnk.util.CompareUtils;
import com.jnk.util.PrintUtils;
import com.jnk.util.TReference;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.lnk.lucene.RunOnce;
import com.lnk.lucene.TempFiles;


public enum SolderRestSkeleton {

	CREATE(SolderRestOp.CREATE,SolderRestSkeleton::doCreate),
	GET(SolderRestOp.GET,SolderRestSkeleton::doGet),	
	GET_LATEST_COMMIT(SolderRestOp.GET_LATEST_COMMIT,SolderRestSkeleton::doGetLatestCommit),
	DOWNLOAD_FILE(SolderRestOp.DOWNLOAD_FILE,SolderRestSkeleton::doDownloadFile),
	GEN_NEW_COMMIT_ID(SolderRestOp.GEN_NEW_COMMIT_ID,SolderRestSkeleton::doGenNewCommitId),
	UPLOAD_FILE(SolderRestOp.UPLOAD_FILE,SolderRestSkeleton::doUploadFile),
	COMMIT_UPLOAD(SolderRestOp.COMMIT_UPLOAD,SolderRestSkeleton::doCommitUpload);

	private static Log LOG = LogFactory.getLog(SolderRestSkeleton.class.getName());
	
	RestOp restOp;
	IOConsumer<RestSkeletonState> cHandler;
	
	private SolderRestSkeleton(RestOp restOp,IOConsumer<RestSkeletonState> cHandler) {
		this.restOp = restOp;
		this.cHandler = cHandler;
	}
	
	
	static final AtomicBoolean s_fInit = new AtomicBoolean(false);
	static Map<String,String> s_mapContentType;

	public static void init() throws IOException {
		LOG.info("SolderRestSkeleton Init called.. isServerInit="+EnServlet.isEnServletInitCalled());
		RunOnce.ensure(s_fInit, () -> {
			
			if (EnServlet.isEnServletInitCalled()) {
				SolderRestSkeleton[] a = SolderRestSkeleton.class.getEnumConstants();
				for (SolderRestSkeleton skel : a) {
					RestProcessor.register(skel.restOp.getOp(), skel.cHandler);
				}
			} else {
				LOG.info("SolderRestSkeleton Init called, No servlet found, Not doing anything..");
			}
		});
		
		

	}
	
	static void ensureTenant(int resourceTenantId,int userTenantId, String objId) throws IOException {
		if (resourceTenantId != userTenantId ) {
			Event.log(EEvent.Security_Warning, resourceTenantId, resourceTenantId, (mb) -> {
				mb.put("obj_id", objId);
				mb.put("obj_tenant_id", resourceTenantId);
				mb.put("user_tenant_id", userTenantId);
			});
			LOG.error(String.format("ensureTenant error; resTenantId=%d, userTenantId=%d objId=%s", resourceTenantId,userTenantId,objId));
			throw new RestException("Invalid object;");
		}
	}
	
	
	static void doSentryCheck(String op,SRepo repo,int tenantId) throws IOException {
		SentryProvider prov = RolePriv.getSentryProvider(SentryProvider.SENTRY_ENIGMA);
		User user = (User)SessionManager.getUser();
		Objects.requireNonNull(user,"user");
		
		tenantId = repo!=null?repo.getTenantId():tenantId;
		String objId = repo!=null?"repo "+repo.getId():"op:"+op;
		ensureTenant(tenantId,user.getTenantId(),objId);
		
		boolean fThrow = false;
		boolean fRole = prov.verifyRole(op, tenantId, Scope.SCOPE_ALL, -1, fThrow);
		boolean fPriv = false;
		if (repo!=null) {
			fPriv = prov.verifyPrivilege(op, tenantId, repo.getSeqId(), fThrow);
		}
		boolean fAllow = fRole || fPriv;
		LOG.info(String.format("SentryCheck: op=%s, fAllow=%s (fRole=%s,fPriv=%s)", op,fAllow,fRole,fPriv));
		if (!fAllow) {
			throw new RestException("Invalid object;");
		}
	}
	
	// Skeletons
	static void doCreate(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		TReference<SRepo> refA = new TReference<>();
		// We take string param val and optional param count
		// and return the same val as an array of count values.
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			String repoId = decoder.readString("id");
			repoId = Validator.require(repoId, "repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			String schema = decoder.readString("tschema");
			schema = Validator.require(schema, "schema", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			int aoId = params.contains("ao_id")?decoder.readInt("ao_id"):0;
			
			SRepo repo = SolderVaultFactory.ensureSRepo(repoId, schema, user.getTenantId(), aoId);
			
						
			Objects.requireNonNull(repo,"repo");
			doSentryCheck(SolderSentryProvider.SOLDEROP_SOLDER_ADMIN,null,user.getTenantId());
			ensureTenant(repo.getTenantId(),user.getTenantId(),repo.getId());
			refA.set(repo);
		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeObject("ret", refA.get(),false);
		});
	}
	
	static void doGet(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		TReference<SRepo> refRepo = new TReference<>();
		// We take string param val and optional param count
		// and return the same val as an array of count values.
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			String repoId = Validator.require(decoder.readString("id"), "repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			SRepo repo = SolderVaultFactory.getRepoById(repoId);
			Objects.requireNonNull(repo,()->"repo "+repoId);
			repo.refresh();
			doSentryCheck(SolderSentryProvider.SOLDEROP_READ,repo,-1);
			refRepo.set(repo);
		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeObject("ret", refRepo.get(),false);
		});
	}
	
	static void doGetLatestCommit(RestSkeletonState state) throws IOException {
		
		SCall scall = (SCall)state.getCallObject();
		
		TReference<SCommit> refA = new TReference<>();
		// We take string param val and optional param count
		// and return the same val as an array of count values.
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			
			String repoId = Validator.require(decoder.readString("id"),"repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			SRepo repo = SolderVaultFactory.getRepoById(repoId);
			Objects.requireNonNull(repo,()->"repo "+repoId);
			//Verify Roles and Priv..
			doSentryCheck(SolderSentryProvider.SOLDEROP_READ,repo,-1);
			
			refA.set(repo.getLatestCommit());
		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeObject("ret", refA.get(),false);
		});
		
	}
	
	static void doDownloadFile(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		TReference<File> refA = new TReference<>();
		TReference<SCommit> refB = new TReference<>();
		
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			
			String repoId = Validator.require(decoder.readString("id"),"repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			SRepo repo = SolderVaultFactory.getRepoById(repoId);
			Objects.requireNonNull(repo,()->"repo "+repoId);
			//Verify Roles and Priv..
			doSentryCheck(SolderSentryProvider.SOLDEROP_READ,repo,-1);
			
			String relPath = decoder.readString("rel_path");
			long blobFsId = decoder.readLong("blob_fsid");
			SCommit scommit = repo.getLatestCommit();
			refB.set(scommit);
			IRepoFileService service = ServerRepoFileService.get();
			refA.set(service.downloadFile(repo, relPath, blobFsId));
			
		});
		
		state.setSuccess((encoder) -> {
			encoder.writeObject("ret", refB.get(),false);
		}, (os)->{
			InputStream is = new FileInputStream(refA.get());
			try {
				IOUtils.copy(is,os);
			}finally {
				IOUtils.closeQuietly(is);
			}
		});
		
	}
	
	static void doGenNewCommitId(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		MutableInt commitId = new MutableInt(-1);
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			
			String repoId = Validator.require(decoder.readString("id"),"repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			SRepo repo = SolderVaultFactory.getRepoById(repoId);
			Objects.requireNonNull(repo,()->"repo "+repoId);
			
			commitId.setValue(repo.generateNewCommitId());
			
			//Verify Roles and Priv..
			doSentryCheck(SolderSentryProvider.SOLDEROP_WRITE,repo,-1);

		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeInt("ret", commitId.intValue());
		});
	}
	
	static File writeTemp(TempFiles tf,String dirName,String fileName,InputStream is,String digestExpect) throws IOException {
		
		
		File fileRoot = tf.getTempDir(dirName);
		File fileTmp = new File(fileRoot,fileName);
		
		
		OutputStream os = null;
		boolean fError = true;
		
		try {
			
			
			Validator.checkFile(fileTmp, "New Temp file");
			os = new FileOutputStream(fileTmp);
			
			MessageDigest md = RemoteRepoSync.tlMessageDigest.get();
			md.reset();
			DigestOutputStream dos = new DigestOutputStream(os, md);
			IOUtils.copy(is, dos);
			dos.close();
			is.close();
			is = null;
			
			byte[] digest = md.digest();
			String digestNew = PrintUtils.toHexString(digest);
			
			if (!StringUtils.isEmpty(digestExpect) && !CompareUtils.stringEquals(digestNew, digestExpect)) {
				String stError = String.format("Upload digest mismatch for %s. writeDigest=%s, prevCalc=%s",
					dirName, digestNew, digestExpect);
				LOG.info(stError);
				throw new RestException(stError);
			}

			fError = false;
			return fileTmp;
		} finally {
			IOUtils.closeQuietly(os);
			if (fError) {
				fileTmp.delete();
				tf.removeTempDir(fileRoot);
			}
		}
			
		
	}
	
	static void doUploadFile(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		TempFiles tf = TempFiles.get(TempFiles.DEFAULT);
		
		
		TReference<SRepo> refSRepo = new TReference<>();
		TReference<SolderEntry> refA = new TReference<>();
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			
			String repoId = Validator.require(decoder.readString("id"),"repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			SRepo repo = SolderVaultFactory.getRepoById(repoId);
			Objects.requireNonNull(repo,()->"repo "+repoId);
			refSRepo.set(repo);
			
			SolderEntry se = decoder.readObject("se",SolderEntry.class);
			Objects.requireNonNull(se,"Solder Entry!");
			refA.set(se);
			//Verify Roles and Priv..
			doSentryCheck(SolderSentryProvider.SOLDEROP_WRITE,repo,-1);
		});
		
		SRepo repo = refSRepo.get();
		SolderEntry se = refA.get();
		File fileTmp =null;
		InputStream is = null;
		try {
			is = state.getRequestInputStream() ;
			fileTmp = writeTemp(tf,repo.getId(),se.getDigest(),is,se.getDigest());
			se.setFile(fileTmp);
			
			//Write a file and give it to the 
			IRepoFileService service = ServerRepoFileService.get();
			service.uploadFile(repo, se);
			
			if (se.getBlobFsId() <=0L) {
				throw new SolderException("Error, bad BlobFSId "+se.getBlobFsId());
			}
			
			state.setSuccess((encoder) -> {
				encoder.writeLong("ret", se.getBlobFsId());
			});
		} finally {
			IOUtils.closeQuietly(is);
			if (fileTmp!=null) {
				fileTmp.delete();
				tf.removeTempDir(fileTmp.getParentFile());
			}
		}
	}
	
	static void doCommitUpload(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		TempFiles tf = TempFiles.get(TempFiles.DEFAULT);
		
		TReference<SRepo> refSRepo = new TReference<>();
		TReference<SCommitInfo> refSci = new TReference<>();
		TReference<String> refDigest = new TReference<>();
		TReference<String[]> refDelRelPaths = new TReference<>();
		
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			
			String repoId = Validator.require(decoder.readString("id"),"repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			SRepo repo = SolderVaultFactory.getRepoById(repoId);
			Objects.requireNonNull(repo,()->"repo "+repoId);
			refSRepo.set(repo);
			
			
			
			SCommitInfo sci = decoder.readObject("sci",SCommitInfo.class);
			Objects.requireNonNull(sci,"SCommitInfo!");
			refSci.set(sci);
			refDigest.set(decoder.readString("digest"));
			refDelRelPaths.set(decoder.readStringArray("del_rel_paths"));
			
			//Verify Roles and Priv..
			doSentryCheck(SolderSentryProvider.SOLDEROP_WRITE,repo,-1);
		});
		
		SRepo repo = refSRepo.get();
		SCommitInfo sci = refSci.get();
		String uploadDigest = refDigest.get();
		String[] aDelRelPaths = refDelRelPaths.get();
		
		File fileTmp =null;
		InputStream is = null;
		try {
			is = state.getRequestInputStream() ;
			fileTmp = writeTemp(tf,repo.getId(),sci.getCHash(),is,uploadDigest);
			
			//Write a file and give it to the 
			IRepoFileService service = ServerRepoFileService.get();
			
			SCommit scommit = (SCommit)service.createSCommit(repo, sci.getCHash(), sci.getInfo(), sci.getId());
			
			List<String> listDelEntryRelPath = aDelRelPaths!=null?List.of(aDelRelPaths):null;
			service.commitUpload(repo, scommit, fileTmp,listDelEntryRelPath);
			state.setSuccess((encoder) -> {
				//blobFsId
				encoder.writeLong("ret", scommit.getBlobFsId());
			});
		} finally {
			IOUtils.closeQuietly(is);
			if (fileTmp!=null) {
				fileTmp.delete();
				tf.removeTempDir(fileTmp.getParentFile());
			}
		}
	}
	
	
	
	
}