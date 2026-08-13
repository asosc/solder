package org.solder.ens;



import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOConsumer;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.core.SCommit;
import org.solder.core.SRepo;
import org.solder.core.ServerRepoFileService;
import org.solder.core.SolderException;
import org.solder.core.SolderSentryProvider;
import org.solder.core.SolderVaultFactory;
import org.solder.rest.client.SolderRestOp;
import org.solder.rest.solder.CommitSession;
import org.solder.rest.solder.IRepoFileService;
import org.solder.rest.solder.SCommitInfo;
import org.solder.rest.solder.SRepoInfo;
import org.solder.rest.solder.SolderEntry;

import com.ee.ens.AbstractHttpServlet.SCall;
import com.ee.ens.beeObj.EEBeeFs;
import com.ee.ens.EnServlet;
import com.ee.rest.RestException;
import com.ee.rest.RestOp;
import com.ee.rest.RestProcessor;
import com.ee.rest.RestSkeletonState;
import com.ee.rest.RestOp.RestClient;
import com.ee.session.SessionManager;
import com.ee.session.db.EEvent;
import com.ee.session.db.EStateObj;
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
	SEARCH(SolderRestOp.SEARCH,SolderRestSkeleton::doSearch),
	UPDATE(SolderRestOp.UPDATE,SolderRestSkeleton::doUpdate),
	DELETE(SolderRestOp.DELETE,SolderRestSkeleton::doDelete),
	GET_LATEST_COMMIT(SolderRestOp.GET_LATEST_COMMIT,SolderRestSkeleton::doGetLatestCommit),
	GET_COMMIT(SolderRestOp.GET_COMMIT,SolderRestSkeleton::doGetCommit),
	DOWNLOAD_FILE(SolderRestOp.DOWNLOAD_FILE,SolderRestSkeleton::doDownloadFile),
	
	BEGIN_COMMIT(SolderRestOp.BEGIN_COMMIT,SolderRestSkeleton::doBeginCommit),
	UPLOAD_FILE(SolderRestOp.UPLOAD_FILE,SolderRestSkeleton::doUploadFile),
	UPLOAD_COMMIT(SolderRestOp.UPLOAD_COMMIT,SolderRestSkeleton::doUploadCommit);

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
	
	static boolean crossTenantCheck() throws IOException {
		SentryProvider prov = RolePriv.getSentryProvider(SentryProvider.SENTRY_ENIGMA);
		User user = (User) SessionManager.getUser();
		Objects.requireNonNull(user, "user");

		return prov.verifyRole(SentryProvider.NAMEDOP_ADMIN, user.getTenantId(), Scope.SCOPE_ALL, -1, false);
	}
	
	static void ensureTenant(int resourceTenantId,int userTenantId, String objId) throws IOException {
		//Cross tenant is availabel..
		
		if (resourceTenantId != userTenantId ) {
			if (!crossTenantCheck()) {
				Event.log(EEvent.Security_Warning, resourceTenantId, resourceTenantId, (mb) -> {
					mb.put("obj_id", objId);
					mb.put("obj_tenant_id", resourceTenantId);
					mb.put("user_tenant_id", userTenantId);
				});
				LOG.error(String.format("ensureTenant error; resTenantId=%d, userTenantId=%d objId=%s", resourceTenantId,userTenantId,objId));
				throw new RestException("Invalid object;");
			} else {
				LOG.info("Cross tenant check passed!");
			}
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
	
	static SRepo getRepo(int sid,String id,boolean fThrow) throws IOException {
		SRepo repo =null;
		if (sid>=0) {
			 repo = SRepo.getRepoBySeqId(sid);
			 if (fThrow) {
				 Objects.requireNonNull(repo,()->"repo "+sid);
			 }
		} else if (id != null && id.isBlank()) {
			repo = SRepo.getRepoById(id);
			if (fThrow) {
				Objects.requireNonNull(repo,()->"repo "+id);
			}
		} else {
			throw new SolderException("No repo id or sid given!");
		}
		
		return repo;
	}
	
	
	
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
			String tSchema = decoder.readString("tschema");
			tSchema = Validator.require(tSchema, "schema", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			int aoId = params.contains("ao_id")?decoder.readInt("ao_id"):0;
			String tag = params.contains("tag")?decoder.readString("tag"):null;
			
			LOG.info(String.format("SolderRest Op: doCreate; repoId=%s tSchema=%s aoId=%d tag=%s", repoId,tSchema,aoId,""+tag));

			// Authorize before any create/update mutation.
			doSentryCheck(SolderSentryProvider.SOLDEROP_SOLDER_ADMIN,null,user.getTenantId());
			
			SRepo repo = SRepo.ensureSRepo(repoId, tSchema, user.getTenantId(), aoId,tag);
			Objects.requireNonNull(repo,"repo");
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
			//User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			int repoSid = params.contains("sid")?decoder.readInt("sid"):-1;
			String repoId = null;
			if (repoSid<=0) {
				repoId = Validator.require(decoder.readString("id"),"repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			}
			SRepo repo = getRepo(repoSid,repoId,true);
			
			
			
			LOG.info(String.format("SolderRest Op: doGet; sid=%d repoId=%s", repoSid,""+repoId));
			
			
			repo.refresh(null);
			doSentryCheck(SolderSentryProvider.SOLDEROP_READ,repo,-1);
			refRepo.set(repo);
		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeObject("ret", refRepo.get(),false);
		});
	}
	
	static void doSearch(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		TReference<SRepoInfo[]> ref = new TReference<>();
		// We take string param val and optional param count
		// and return the same val as an array of count values.
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			
			int tenantId = user.getTenantId();
			String repoIdWild = null;
			String schemaWild = null;
			String tagFilter = null;
			
			if (params.contains("idWild")) {
				repoIdWild = decoder.readString("idWild");
			}
			if (params.contains("tschemaWild")) {
				schemaWild = decoder.readString("tschemaWild");
			}
			
			if (params.contains("tagFilter")) {
				tagFilter = decoder.readString("tagFilter");
			}
			
			LOG.info(String.format("SolderRest Op: doSearch; repoIdWild=%s schemaWild=%s tagFilter=%s", ""+repoIdWild,""+schemaWild,""+tagFilter));
			
			doSentryCheck(SolderSentryProvider.SOLDEROP_READ,null,tenantId);
			
			List<SRepo> list = SRepo.searchRepo(tenantId, repoIdWild, schemaWild,tagFilter);
			SRepoInfo[] a = list.stream().map((r)->SRepo.makeSRepoInfo(r)).toArray(SRepoInfo[]::new);
			ref.set(a);
		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeObjectArray("ret", ref.get(),false);
		});
	}

	static void doUpdate(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		TReference<SRepoInfo> refRepo = new TReference<>();
		// We take string param val and optional param count
		// and return the same val as an array of count values.
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			//User user = (User)SessionManager.getUser();
			
			
			Set<String> params =decoder.getAllObjectFields();
			int repoSid = params.contains("sid")?decoder.readInt("sid"):-1;
			String repoId = null;
			if (repoSid<=0) {
				repoId = Validator.require(decoder.readString("id"),"repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			}
			SRepo repo = getRepo(repoSid,repoId,true);
			
			String tagNew = decoder.readString("tag");
			Objects.requireNonNull(tagNew,"tagNew is null!");
			
			LOG.info(String.format("SolderRest Op: doGet; sid=%d repoId=%s", repoSid,""+repoId));
					
			doSentryCheck(SolderSentryProvider.SOLDEROP_SOLDER_ADMIN,repo,-1);
			
			repo.refresh(null);
			
			repo.updateChange(tagNew, null);
			refRepo.set(SRepo.makeSRepoInfo(repo));
			
		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeObject("ret", refRepo.get(),false);
		});
	}
	
	static void doDelete(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		TReference<SRepoInfo> refRepo = new TReference<>();
		// We take string param val and optional param count
		// and return the same val as an array of count values.
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			int repoSid = params.contains("sid")?decoder.readInt("sid"):-1;
			String repoId = null;
			if (repoSid<=0) {
				repoId = Validator.require(decoder.readString("id"),"repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			}
			SRepo repo = getRepo(repoSid,repoId,false);
			
			LOG.info(String.format("SolderRest Op: doDelete; sid=%d repoId=%s", repoSid,""+repoId));
			
			if (repo!=null) {
				repo.refresh(null);
				doSentryCheck(SolderSentryProvider.SOLDEROP_SOLDER_ADMIN,repo,-1);
				repo.updateDelete();
			} else 	{
				List<SRepo> list = SRepo.getDeletedRepo(user.getTenantId(),repoId);
				if (list!=null && list.size()>0) {
					repo = list.get(0);
				} else {
					throw new RestException("Unable to find repo  "+repoId);
				}
			}
			
			refRepo.set(SRepo.makeSRepoInfo(repo));
		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeObject("ret", refRepo.get(),false);
		});
	}
	
	
	
	
	
	static void doGetLatestCommit(RestSkeletonState state) throws IOException {
		
		SCall scall = (SCall)state.getCallObject();
		
		TReference<SCommitInfo> ref = new TReference<>();
		// We take string param val and optional param count
		// and return the same val as an array of count values.
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			//User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			int repoSid = params.contains("sid")?decoder.readInt("sid"):-1;
			String repoId = null;
			if (repoSid<=0) {
				repoId = Validator.require(decoder.readString("id"),"repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			}
			SRepo repo = getRepo(repoSid,repoId,true);
			
			LOG.info(String.format("SolderRest Op: doGetLatestCommit; sid=%d repoId=%s", repoSid,""+repoId));
			
			//Verify Roles and Priv..
			doSentryCheck(SolderSentryProvider.SOLDEROP_READ,repo,-1);
			
			ref.set(SCommit.makeSCommitInfo(repo.getLatestCommit()));
		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeObject("ret", ref.get(),false);
		});
		
	}
	
	
	static void doGetCommit(RestSkeletonState state) throws IOException {
		
		SCall scall = (SCall)state.getCallObject();
		
		TReference<SCommitInfo[]> ref = new TReference<>();
		// We take string param val and optional param count
		// and return the same val as an array of count values.
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			//User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			int repoSid = params.contains("sid")?decoder.readInt("sid"):-1;
			String repoId = null;
			if (repoSid<=0) {
				repoId = Validator.require(decoder.readString("id"),"repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			}
			SRepo repo = getRepo(repoSid,repoId,true);
			
			int[] commitIds = params.contains("commits")?decoder.readIntArray("commits"):null;
			if (commitIds!=null && commitIds.length==0) {
				commitIds=null;
			}
			
			LOG.info(String.format("SolderRest Op: doGetAllCommit; sid=%d repoId=%s commitIds=%s", repoSid,""+repoId,Arrays.toString(commitIds)));
			
			
			//Verify Roles and Priv..
			doSentryCheck(SolderSentryProvider.SOLDEROP_READ,repo,-1);
			List<SCommit> list = repo.getAllCommit();
			
			
			if (commitIds == null) {
				ref.set(list.stream().map((r)->SCommit.makeSCommitInfo(r)).toArray(SCommitInfo[]::new));
			} else {
				SCommitInfo[] a = new SCommitInfo[commitIds.length];
				for (int i=0;i<commitIds.length;i++) {
					int id = commitIds[i];
					SCommit sci = list.stream().filter((c)->c.getId()==id).findFirst().get();
					a[i] = SCommit.makeSCommitInfo(Objects.requireNonNull(sci,()->"commit "+id));
				}
				ref.set(a);
			}
		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeObjectArray("ret", ref.get(),false);
		});
		
	}
	
	static void doDownloadFile(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		TReference<File> ref = new TReference<>();
		
		
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			//User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			int repoSid = params.contains("sid")?decoder.readInt("sid"):-1;
			String repoId = null;
			if (repoSid<=0) {
				repoId = Validator.require(decoder.readString("id"),"repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			}
			
			String relPath = decoder.readString("rel_path");
			long blobFsId = decoder.readLong("blob_fsid");
			String digestExpected = params.contains("digest_expect")?decoder.readString("digest_expect") :null;
			
			if (blobFsId<=0) {
				throw new SolderException(String.format("Invalid blobFsId %d",blobFsId));
			}

			
			LOG.info(String.format("SolderRest Op: doDownloadFile; sid=%d repoId=%s replPath+%s blobFsId=%d digestExpected=%s", repoSid,""+repoId,relPath,blobFsId,digestExpected));
			
			SRepo repo = getRepo(repoSid,repoId,true);
		
			//Verify Roles and Priv..
			doSentryCheck(SolderSentryProvider.SOLDEROP_READ,repo,-1);
			
			
			if (!StringUtils.isEmpty(relPath)) {
				SolderEntry.requireSafeRelPath(relPath);
			} 
			
			ref.set(repo.downloadFile(relPath, blobFsId,digestExpected));
			
		});
		
		state.setSuccess((encoder) -> {
			encoder.writeString("ret", "success");
		}, (os)->{
			InputStream is = new FileInputStream(ref.get());
			try {
				IOUtils.copy(is,os);
			}finally {
				IOUtils.closeQuietly(is);
			}
		});
		
	}
	
	static void doBeginCommit(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		TReference<CommitSession> ref = new TReference<>();
		state.readParam((decoder) -> {
			scall.handleSession(decoder,null,false);
			//User user = (User)SessionManager.getUser();
			
			//Set<String> params =decoder.getAllObjectFields();
			
			SCommitInfo commitInfoReq = decoder.readObject("commit_req",SCommitInfo.class);
			String[] aStRelPathMod = decoder.readStringArray("rpath_add");
			String[] aStRelPathDel = decoder.readStringArray("rpath_del");
			
			//RelPath are client managed, but could be used to verify the chash provided by client (Allows servers to keep
			//consistent behavior across all clients. May be later..
			
			LOG.info(String.format("SolderRest Op: doBeginCommit; sid=%d cHash=%s ",commitInfoReq.getRepoSeqId(),commitInfoReq.getCHash()));
			
			SRepo repo = getRepo(commitInfoReq.getRepoSeqId(),null,true);
			

			// Authorize before allocating a commit sequence id.
			doSentryCheck(SolderSentryProvider.SOLDEROP_WRITE,repo,-1);
			
			
			@SuppressWarnings("resource")
			SSCommit ssc = new SSCommit(repo,commitInfoReq,aStRelPathMod,aStRelPathDel);
			
			ref.set(ssc.getCommitSession());

		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeObject("ret", ref.get(),false);
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
			
			MessageDigest md = SolderEntry.tlMessageDigest.get();
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
	
	static String requireEcid(String ecid) {
		return Validator.require(ecid, "ecid", Rules.NO_NULL_EMPTY, Rules.TRIM);
	}
	
	static void doUploadFile(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		TempFiles tf = TempFiles.get(TempFiles.DEFAULT);
		
		
		TReference<SSCommit> refSsc = new TReference<>();
		TReference<SolderEntry> refA = new TReference<>();
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			//User user = (User)SessionManager.getUser();
			
			//Set<String> params =decoder.getAllObjectFields();
			String ecid = requireEcid(decoder.readString("ecid"));
			SolderEntry se = decoder.readObject("se",SolderEntry.class);
			Objects.requireNonNull(se,"Solder Entry!");
			LOG.info(String.format("SolderRest Op: doUploadFile; ecid=%s se=(relPath=%s,size=%,d,digest=%s)",ecid,se.getRelPath(),se.getSize(),se.getDigest()));
			
			
			
			
			SSCommit ssc = EStateObj.get(ecid);
			Objects.requireNonNull(ssc,()->String.format("SSCommit ecid %s not found. Invalid or expired!", ecid));
			
			doSentryCheck(SolderSentryProvider.SOLDEROP_WRITE,ssc.srepo,-1);
			refSsc.set(ssc);
			refA.set(se);
			//Verify Roles and Priv..
			
		});
		
		SSCommit ssc = refSsc.get();
		SolderEntry se = refA.get();
		File fileTmp =null;
		InputStream is = null;
		try {
			is = state.getRequestInputStream() ;
			fileTmp = writeTemp(tf,ssc.srepo.getId(),se.getDigest(),is,se.getDigest());
			se.setFile(fileTmp);
			long blobFsId = ssc.upload(se,fileTmp);
			
			
			if (blobFsId <=0L) {
				throw new SolderException("Error, bad BlobFSId "+blobFsId);
			}
			
			state.setSuccess((encoder) -> {
				encoder.writeLong("ret", blobFsId);
			});
		} finally {
			IOUtils.closeQuietly(is);
			if (fileTmp!=null) {
				fileTmp.delete();
				tf.removeTempDir(fileTmp.getParentFile());
			}
		}
	}
	
	
	
	static void doUploadCommit(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		
		
		TempFiles tf = TempFiles.get(TempFiles.DEFAULT);
		
		TReference<SSCommit> refSsc = new TReference<>();
		TReference<String> refDigest = new TReference<>();
		
		
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			//User user = (User)SessionManager.getUser();
			//Set<String> params =decoder.getAllObjectFields();
			
			String ecid = requireEcid(decoder.readString("ecid"));
			String digest = requireEcid(decoder.readString("digest"));
			
			LOG.info(String.format("SolderRest Op: doUploadCommit; ecid=%s digest=%s",ecid,digest));
			
			SSCommit ssc = EStateObj.get(ecid);
			Objects.requireNonNull(ssc,()->String.format("SSCommit ecid %s not found. Invalid or expired!", ecid));
			
			doSentryCheck(SolderSentryProvider.SOLDEROP_WRITE,ssc.srepo,-1);
			refSsc.set(ssc);
			refDigest.set(digest);
			
		});
		
		
		SSCommit ssc = refSsc.get();
		String digest = refDigest.get();
		File fileTmp =null;
		InputStream is = null;

		try {
			is = state.getRequestInputStream() ;
			fileTmp = writeTemp(tf,ssc.srepo.getId(),ssc.commitId+"_"+digest,is,digest);
			SCommit scommit = ssc.uploadCommit(fileTmp);
			SCommitInfo sciRet = SCommit.makeSCommitInfo(Objects.requireNonNull(scommit));
			
			state.setSuccess((encoder) -> {
				//blobFsId
				encoder.writeObject("ret", sciRet,false);
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