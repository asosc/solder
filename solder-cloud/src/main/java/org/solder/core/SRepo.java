package org.solder.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nimbo.blobs.BlobFS;
import org.nimbo.blobs.BlobFile;
import org.nimbo.blobs.BlobFileTransact;
import org.nimbo.blobs.Container;
import org.nimbo.blobs.ContainerGroup;
import org.solder.rest.solder.IRepoFileService;
import org.solder.rest.solder.RemoteRepoSync;
import org.solder.rest.solder.SCommitInfo;
import org.solder.rest.solder.SLocalRepo;
import org.solder.rest.solder.SRepoInfo;
import org.solder.rest.solder.SolderEntry;
import org.solder.vsync.SyncLocalRepo;

import com.aura.crypto.CryptoScheme;
import com.beech.bfs.Mode;
import com.beech.store.FileVaultProvider;
import com.beech.store.IVaultProvider;
import com.beech.store.TVault;
import com.ee.rest.RestException;
import com.ee.session.SQLTm;
import com.ee.session.SessionManager;
import com.ee.session.db.Audit;
import com.ee.session.db.EEvent;
import com.ee.session.db.Event;
import com.ee.session.db.Tenant;
import com.jnk.util.CompareUtils;
import com.jnk.util.PrintUtils;
import com.jnk.util.TReference;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.jnk.util.cache.Cache;
import com.jnk.util.cache.CacheHelper;
import com.lnk.jdbc.DBType;
import com.lnk.jdbc.DriverUtil;
import com.lnk.jdbc.MSSQLUtil;
import com.lnk.jdbc.SQLDatabase;
import com.lnk.jdbc.SQLQuery;
import com.lnk.jdbc.SQLTableSchema;
import com.lnk.jdbc.SQLUtil;
import com.lnk.lucene.BackgroundTask;
import com.lnk.lucene.BitUtil;
import com.lnk.lucene.RunOnce;
import com.lnk.lucene.record.RecordUtil;
import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;
import com.lnk.serializer.ISerializable;

public class SRepo implements ISerializable {

	private static Log LOG = LogFactory.getLog(SRepo.class.getName());
	
	public static SRepoInfo makeSRepoInfo(SRepo srepo) {
		
		if (srepo==null) {
			return null;
		} else {
			SRepoInfo repoInfo = new SRepoInfo(srepo.sid, srepo.id, srepo.tSchema, srepo.tenantId, srepo.aoId,srepo.tag,srepo.commitDir,srepo.commitId,
				srepo.dateCommit,srepo.dateChange,srepo.dateCreate,srepo.dateUpdate);
			repoInfo.setParent(srepo);
			return repoInfo;
		}
	}
	
	public static SRepo getSRepo(SRepoInfo srepoInfo) throws IOException {
		//No null support, apps can do that this is most common situation and we want early error.
		Objects.requireNonNull(srepoInfo,"srepo");
		SRepo repo = (SRepo)srepoInfo.getParent();
		if (repo == null) {
			//probably throw...(only efficiency issue...
			repo = SRepo.getRepoBySeqId(srepoInfo.getSeqId());
			Objects.requireNonNull(repo,"srepo reload");
		}
		return repo;
	}

	static final String SREPO_TABLE = "srepo";
	static final String SREPO_SEQ = "srepo_seq";
	static final String KEY_SID = "sid";
	
	static final String BLOB_TYPE_SOLDER_REPO = "solder_repo";
	static final String BLOB_TYPE_SOLDER_COMMIT = "solder_commit";

	static AtomicBoolean s_fInit = new AtomicBoolean(false);
	static RepQueries repQ = null;
	static Cache<SRepo> cacheRepo = null;

	public static void init(SQLDatabase db) throws IOException {
		SQLDatabase dbFinal = Objects.requireNonNull(db, "db");
		RunOnce.ensure(s_fInit, () -> {
			repQ = new RepQueries(dbFinal.getName(), dbFinal.getType());
			SCommit.init(dbFinal);
			SolderSentryProvider.init();
		});
	}
	

	public static class RepQueries {

		SQLTableSchema tsRepo;

		SQLQuery qRepoIns, qRepoSelId, qRepoSelSid, qRepoSelUnique, qRepoSelTenant, qRepoUpdCommit, qRepoUpdChange,
				qRepoUpdDel, qRepoDelOne, qRepoSeq;

		RepQueries(String dbName, DBType dbType) throws IOException {

			// (name,fieldType(canonicalName),[flags(0,1),nSpit])

			tsRepo = new SQLTableSchema(SREPO_TABLE);
			tsRepo.parseAndAdd(new String[] { "sid,int,1", "id,string,1", "tschema,string(128),1", "tenant_id,int,1",
					"ao_id,int,1", "tag,string,0", "deleted,boolean,1", "commit_dir,string,0", 
					"commit_id,int,1", "commit_date,date,1", "change_date,date,1", "create_date,date,1",
					"last_update,date,1" });

			String stPrimaryKey = "sid";
			String[] aUnique = new String[] { "tenant_id,tschema,ao_id", "id" };
			String[] aIndex = null;
			tsRepo.setCreateScriptParams(stPrimaryKey, aUnique, aIndex, Tenant.FILE_GROUP, SREPO_SEQ);
			tsRepo.setReadOnly();
			SQLTableSchema.register(tsRepo);

			// This is only for logging (Developers can use this to create scripts suitable
			// to any
			// supported database.
			MSSQLUtil.getCreateTableScript(tsRepo);
			MSSQLUtil.getCreateSequenceScript(SREPO_SEQ);

			qRepoSeq = DriverUtil.createSequenceQuery(dbName, dbType, tsRepo, SREPO_SEQ);
			qRepoIns = DriverUtil.createInsertQuery(dbName, dbType, tsRepo);
			qRepoSelId = DriverUtil.createSelectQuery(dbName, dbType, tsRepo, "id", "ById");
			qRepoSelSid = DriverUtil.createSelectQuery(dbName, dbType, tsRepo, "sid", "BySid");
			qRepoSelUnique = DriverUtil.createSelectQuery(dbName, dbType, tsRepo, "tenant_id,tschema,ao_id",
					"ByUnique");
			qRepoSelTenant = DriverUtil.createSelectQuery(dbName, dbType, tsRepo, "tenant_id,deleted", "ByTenant");

			qRepoUpdCommit = DriverUtil.createUpdateQuery(dbName, dbType, tsRepo, "commit_id,commit_date,last_update",
					"sid,commit_id", "Commit");
			qRepoUpdChange = DriverUtil.createUpdateQuery(dbName, dbType, tsRepo, "tag,change_date,last_update", "sid",
					"Change");

			qRepoUpdDel = DriverUtil.createUpdateQuery(dbName, dbType, tsRepo, "id,tschema,deleted,last_update",
					"sid,id", "Del");

			qRepoDelOne = DriverUtil.createDeleteQuery(dbName, dbType, tsRepo, "sid", "One");
			SQLQuery.addToMap(qRepoIns, qRepoSelId, qRepoSelUnique, qRepoUpdCommit, qRepoUpdChange, qRepoDelOne);

			cacheRepo = BackgroundTask.get().createCache(SREPO_TABLE, true);

		}
	}

	static int generateSRepoId() throws IOException {
		return (int) SQLTm.get().nextSequenceId(repQ.qRepoSeq);
	}

	protected int sid;
	protected String id, tSchema, tag, commitDir;
	protected boolean fDeleted;
	protected int tenantId, aoId, commitId;
	protected Date dateCommit, dateChange, dateCreate, dateUpdate;

	public SRepo() {
	}

	public SRepo(String id, String schemaName, int tenantId, int aoId, String tag, String commitDir) throws IOException {
		super();
		this.id = Validator.require(id, "id", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
		this.tSchema = Validator.require(schemaName, "schema name", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);

		Validator.require(id.length(), 2, 48, "id Length", Rules.MIN_MAX);
		Validator.require(schemaName.length(), 1, 32, "schema name Length", Rules.MIN_MAX);

		if (commitDir == null) {
			commitDir = "";
		} else {
			commitDir = commitDir.trim();
		}
		this.commitDir = commitDir;

		this.tenantId = tenantId;
		this.aoId = aoId;
		this.tag = tag;
		commitId = 0;
		dateCreate = new Date();
		dateUpdate = dateCreate;
		// Just put date rep some number less than create..
		dateCommit = new Date(dateCreate.getTime() - TimeUnit.DAYS.toMillis(30));
		dateChange = dateCreate;
		// Create scache before you insert..
		getProvider(false);
		this.sid = generateSRepoId();
		insert();
	}

	public void serialize(Encoder encoder) throws IOException {
		encoder.writeInt("sid", sid);
		encoder.writeString("id", id);
		encoder.writeString("tschema", tSchema);
		encoder.writeInt("tenant_id", tenantId);
		encoder.writeInt("ao_id", aoId);
		encoder.writeString("tag", tag);
		encoder.writeBoolean("deleted", fDeleted);
		encoder.writeString("commit_dir", commitDir);
		encoder.writeInt("commit_id", commitId);
		encoder.writeDate("commit_date", dateCommit);
		encoder.writeDate("change_date", dateChange);
		encoder.writeDate("create_date", dateCreate);
		encoder.writeDate("last_update", dateUpdate);
	}

	public void deserialize(Decoder decoder) throws IOException {
		sid = decoder.readInt("sid");
		id = decoder.readString("id");
		tSchema = decoder.readString("tschema");
		tenantId = decoder.readInt("tenant_id");
		aoId = decoder.readInt("ao_id");
		tag = decoder.readString("tag");
		fDeleted = decoder.readBoolean("deleted");
		commitDir = decoder.readString("commit_dir");
		commitId = decoder.readInt("commit_id");
		dateCommit = decoder.readDate("commit_date");
		dateChange = decoder.readDate("change_date");
		dateCreate = decoder.readDate("create_date");
		dateUpdate = decoder.readDate("last_update");
	}

	public String[] cacheKeys() {
		return new String[] { CacheHelper.getKey(CacheHelper.KEY_ID, id),
				CacheHelper.getKey(CacheHelper.KEY_TENANT_TYPE_NAME, tenantId, tSchema, String.valueOf(aoId)),
				CacheHelper.getKey(KEY_SID, sid) };
	}

	public synchronized void refresh(IRepoFileService rfs) throws IOException {
		// rfs is ignored as this is a server object.
		SRepo srepo = selectRepoById(id, this);
		if (srepo != this) {
			throw new IOException("Unable to refresh lock id=" + id);
		}
	}

	public synchronized int getSeqId() {
		return sid;
	}

	public synchronized String getId() {
		return id;
	}

	public synchronized String getName() {
		return getId();
	}

	public synchronized String getTSchema() {
		return tSchema;
	}

	public synchronized int getTenantId() {
		return tenantId;
	}

	public synchronized int getAoId() {
		return aoId;
	}

	public synchronized String getTag() {
		return tag;
	}

	public synchronized String getCommitDir() {
		return commitDir;
	}


	public synchronized int getCommitId() {
		return commitId;
	}

	/**
	 * Optimistic concurrency: client must still be based on the current tip.
	 * Call at beginCommit (fail fast) and again at commitUpload (tip may have moved).
	 */
	public synchronized void requireExpectedTip(int idPrevExpected) throws IOException {
		refresh(null);
		if (idPrevExpected != commitId) {
			throw new SolderException(String.format(
					"Stale commit base; client prev_id=%d server tip=%d repo=%s(%d)",
					idPrevExpected, commitId, id, sid));
		}
	}

	public synchronized Date getCommitDate() {
		return dateCommit;
	}

	public synchronized Date getChangeDate() {
		return dateChange;
	}

	public synchronized Date getCreateDate() {
		return dateCreate;
	}

	public synchronized Date getLastDate() {
		return dateUpdate;
	}

	public String toString() {
		return RecordUtil.printJson(this, false);
	}

	void insert() throws IOException {
		// We get the transactions sqlTm.
		if (this.sid <= 0) {
			throw new SolderException("SRepo Id not set");
		}

		SQLTm.get().executeOne(repQ.qRepoIns, this::serialize, null);

		// Add to the cache
		cacheRepo.store(this, this::cacheKeys);
		Audit.audit(SAudit.SRepo_Create, sid, -1, tenantId, (cmb) -> {
			cmb.put("id", id);
			cmb.put("schema", tSchema);
		});
	}

	public synchronized void updateChange(String tagNew, Date dateChange) throws IOException {

		String tagFinal = Validator.updateValue(tagNew, tag, null);
		Date dateChangeFinal = Validator.updateValue(dateChange, this.dateChange, null);
		Date dateUpdateNew = new Date();

		int i = SQLTm.get().executeOne(repQ.qRepoUpdChange, (encoder) -> {
			encoder.writeString("tag", tagFinal);
			encoder.writeDate("change_date", dateChangeFinal);
			encoder.writeDate("last_update", dateUpdateNew);

			// Where clause come
			encoder.writeInt("sid", sid);

		}, null);
		if (i == 1) {
			Audit.audit(SAudit.SRepo_Update, sid, -1, tenantId, (cmb) -> {
				cmb.put("id", id);
				cmb.put("op", "change_date");
				cmb.putIfChanged("change_date", dateChangeFinal, this.dateChange);
				cmb.putIfChanged("change_tag", tagFinal, tag);
			});
			this.tag = tagFinal;
			this.dateChange = dateChangeFinal;
			this.dateUpdate = dateUpdateNew;

		} else {
			Event.log(SEvent.DbUpdateFail, sid, tenantId, (mb) -> {
				mb.put("table", "srepo");
				mb.put("id", id);
			});
			throw new SolderException("Failed to update srepo change; id=" + id);
		}

	}

	public synchronized void updateDelete() throws IOException {
		if (fDeleted) {
			throw new SolderException("Repo " + id + " already deleted!");
		}

		String stUnique = CryptoScheme.getDefault().getUUID().substring(1, 5);
		DateFormat df = new SimpleDateFormat("yyMMddHH");

		String suffix = "_" + stUnique + "_" + df.format(new Date());

		String idDel = id + suffix;
		String schemaDel = tSchema + suffix;

		boolean fDeleteNow = true;
		Date dateUpdateNew = new Date();

		int i = SQLTm.get().executeOne(repQ.qRepoUpdDel, (encoder) -> {
			encoder.writeString("id", idDel);
			encoder.writeString("tschema", schemaDel);
			encoder.writeBoolean("deleted", fDeleteNow);

			encoder.writeDate("last_update", dateUpdateNew);

			// Where clause come
			encoder.writeInt("sid", sid);
			encoder.writeString("id", this.id);

		}, null);
		if (i == 1) {
			Audit.audit(SAudit.SRepo_Update, sid, -1, tenantId, (cmb) -> {
				cmb.put("op", "update_del");
				cmb.putIfChanged("id", this.id, idDel);
				cmb.putIfChanged("tschema", this.tSchema, schemaDel);
				cmb.putIfChanged("deleted", this.fDeleted, fDeleteNow);

			});

			cacheRepo.remove(CacheHelper.getKey(KEY_SID, sid));

			this.id = idDel;
			this.tSchema = schemaDel;
			this.fDeleted = fDeleteNow;
			this.dateUpdate = dateUpdateNew;

		} else {
			Event.log(SEvent.DbUpdateFail, sid, tenantId, (mb) -> {
				mb.put("table", "srepo");
				mb.put("op", "update_del");
				mb.put("id", id);
			});
			throw new SolderException("Failed to soft-delete srepo; id=" + id);
		}

	}

	public synchronized void updateCommit(SCommit commit) throws IOException {

		Objects.requireNonNull(commit);

		if (this.commitId > 0 && commitId != commit.getPrevId()) {
			throw new SolderException(
					"CommitId mismatch; current=" + commitId + "; given SCommit prev=" + commit.getPrevId());
		}

		int commitIdNew = commit.getId();
		Date dateCommitNew = commit.getCreateDate();
		if (commitIdNew <= this.commitId) {
			throw new SolderException("Commit id sequencing error; new commit " + commitIdNew
					+ " must be newer than current commit " + commitId);
		}

		Date dateUpdateNew = new Date();
		int expectedTip = this.commitId;

		int i = SQLTm.get().executeOne(repQ.qRepoUpdCommit, (encoder) -> {
			encoder.writeInt("commit_id", commitIdNew);
			encoder.writeDate("commit_date", dateCommitNew);
			encoder.writeDate("last_update", dateUpdateNew);

			// Where clause come
			encoder.writeInt("sid", sid);
			encoder.writeInt("commit_id", expectedTip);

		}, null);
		if (i == 1) {
			Audit.audit(SAudit.SRepo_Update, sid, -1, tenantId, (cmb) -> {
				cmb.put("id", id);
				cmb.put("op", "rep_info");
				cmb.putIfChanged("commit_id", commitIdNew, expectedTip);
				cmb.putIfChanged("commit_date", dateCommitNew, dateCommit);

			});
			this.commitId = commitIdNew;
			this.dateCommit = dateCommitNew;
			this.dateUpdate = dateUpdateNew;
			this.scommit=commit;

		} else {
			// Likely lost optimistic CAS to another writer; refresh for accurate tip in the error.
			refresh(null);
			Event.log(SEvent.DbUpdateFail, sid, tenantId, (mb) -> {
				mb.put("table", "srepo");
				mb.put("id", id);
				mb.put("commit_id", commitIdNew);
				mb.put("expected_tip", expectedTip);
				mb.put("actual_tip", commitId);
			});
			throw new SolderException(String.format(
					"Stale commit tip; update lost race repo=%s(%d) expectedTip=%d actualTip=%d newCommitId=%d",
					id, sid, expectedTip, commitId, commitIdNew));
		}

	}

	// To be used by PURGE When it is implemented..
	void delete() throws IOException {
		int i = SQLTm.get().executeOne(repQ.qRepoDelOne, (encoder) -> {
			encoder.writeInt("sid", sid);
		}, null);

		if (i < 1) {
			Event.log(EEvent.DbDeleteFail, sid, tenantId, (mb) -> {
				mb.put("table", "srepo`");
				mb.put("id", id);
			});
		} else {
			Audit.audit(SAudit.SRepo_Delete, sid, -1, tenantId, (cmb) -> {
				cmb.put("id", id);
				cmb.put("schema", tSchema);
			});
		}

	}

	void verifyCommit(SCommit sc) throws IOException {

		if (sc.getRepoSeqId() != sid) {
			throw new SolderException(
					String.format("Invalid commit %d repoSid got %d expect=%d", sc.getId(), sc.getRepoSeqId(), sid));
		}
	}

	SCommit scommit;

	public synchronized SCommit getLatestCommit() throws IOException {

		// we can even send null as the parameter is ignored.
		refresh(null);

		if (commitId <= 0) {
			return null;
		}

		if (scommit != null) {
			if (scommit.getId() != this.commitId) {
				scommit = null;
			}
		}
		if (scommit == null) {
			SCommit sc = SCommit.selectCommitById(commitId);
			verifyCommit(sc);
			scommit = sc;
		}
		Objects.requireNonNull(scommit, "scommit " + commitId);

		return scommit;
	}

	public List<SCommit> getAllCommit() throws IOException {
		List<SCommit> listCommits = SCommit.selectCommitByRepo(sid);
		Collections.sort(listCommits);
		for (SCommit sc : listCommits) {
			verifyCommit(sc);
		}
		return listCommits;
	}
	
	public SCommit getCommit(int commitId) throws IOException {
		if (commitId<=0) {
			return getLatestCommit();
		} else {
			SCommit sc = SCommit.selectCommitById(commitId);
			verifyCommit(sc);
			return Objects.requireNonNull(sc, "sc " + commitId);
		}
	}
	
	
	BlobFS getBlobFS(long blobFsId) throws IOException {
		BlobFS blobFs = BlobFS.getById(blobFsId);
		Objects.requireNonNull(blobFs, "blobFs " + blobFsId);

	
		// We can use only sid going forward..
		boolean fOwnerMatch = blobFs.getOwnerApp().equals(BLOB_TYPE_SOLDER_REPO) && blobFs.getOwnerRef().equals(Integer.toString(sid));
		if (!fOwnerMatch) {
			LOG.error(String.format("Incorrect BlobFs call; id=%d; ownerApp=%s ownerRef=%s expect=%d", blobFsId,
					blobFs.getOwnerApp(), blobFs.getOwnerRef(), sid));
			throw new SolderException("BlobFsId mismatch for " + blobFsId);
		}
		return blobFs;
	}
	
	public File downloadFile(String relPath,long blobFsId,String digestExpected) throws IOException {
		if (StringUtils.isEmpty(relPath)) {
			// Commit package blob (historical or tip). Ownership is solder_commit + this repo.
			BlobFS blobFs = BlobFS.getById(blobFsId);
			Objects.requireNonNull(blobFs, () -> "commit blobFs " + blobFsId);
			// We can use only sid going forward..
			boolean fOwnerMatch = blobFs.getOwnerApp().equals(BLOB_TYPE_SOLDER_COMMIT)
					&& blobFs.getOwnerRef().equals(Integer.toString(sid));
			if (!fOwnerMatch) {
				LOG.error(String.format("Incorrect commit BlobFs call; id=%d; ownerApp=%s ownerRef=%s expect=%d",
						blobFsId, blobFs.getOwnerApp(), blobFs.getOwnerRef(), sid));
				throw new SolderException("Commit BlobFsId mismatch for " + blobFsId);
			}
			String stDigest = blobFs.getInfo().get("digest");
			if (!StringUtils.isEmpty(digestExpected) && !StringUtils.isEmpty(stDigest)) {
				if (!CompareUtils.stringEquals(stDigest, digestExpected)) {
					throw new RestException(
							String.format("Digest expect error got %s (expect=%s)", stDigest, digestExpected));
				}
			}
			BlobFile blobFile = Container.read(blobFs);
			return blobFile.getFile();
		} else {
			//These are not...
			
			BlobFS blobFs = getBlobFS(blobFsId);
			Objects.requireNonNull(blobFs,()->"BlobFS "+blobFsId);
			String stDigest = blobFs.getInfo().get("digest");
			if (!StringUtils.isEmpty(digestExpected) && !StringUtils.isEmpty(stDigest) ) {
				if (!CompareUtils.stringEquals(stDigest, digestExpected)) {
					throw new RestException(String.format("Digest expect error got %s (expect=%s)",stDigest,digestExpected));
				}
			}
			BlobFile blobFile = Container.read(blobFs);
			return blobFile.getFile();
		}
	}
	
	//Can be the Single InstanceKey for the repo..
			//If the same file is in multiple relPath
			//this provide the dedup key.
	public String computeBlobFsKey(SolderEntry se) {
		MessageDigest md = SolderEntry.tlMessageDigest.get();
		md.reset();
				
		byte[] a = PrintUtils.fromHexString(se.getDigest());
		md.update(a);
		BitUtil.VH_LE_INT.set(a, 0, sid);
		md.update(a, 0, 4);
		byte[] digest = md.digest();
		return PrintUtils.toHexString(digest);
	}
	
	public long uploadFile(SolderEntry se,File fileContent) throws IOException {
		
		Objects.requireNonNull(se,"SolderEntry");
		
		ContainerGroup cg = SolderMain.getSolderCg();
		Objects.requireNonNull(cg,()->SolderMain.SOLDER_CGREG_NAME+" registry setting");
		

		
		CryptoScheme cs = CryptoScheme.getDefault();
		//We use repoKey going forward
		String name = computeBlobFsKey(se);
		
		BlobFS blob = BlobFS.selectByName(name);
		if (blob != null) {
			// We can use only sid going forward..
			boolean fOwnerMatch = blob.getOwnerApp().equals(BLOB_TYPE_SOLDER_REPO)
					&& blob.getOwnerRef().equals(Integer.toString(sid));
			if (!fOwnerMatch) {
				LOG.error(String.format("Incorrect BlobFs call; id=%d; ownerApp=%s ownerRef=%s expect=%d", blob.getId(),
						blob.getOwnerApp(), blob.getOwnerRef(), sid));
				throw new SolderException("BlobFsId mismatch for " + name);
			}
			return blob.getId();
		}
				
				

		Map<String, String> mapInfo = new HashMap<>();
		mapInfo.put("path", se.getRelPath());
		mapInfo.put("pid", SessionManager.getPid());

		
		//If fileRep is null, it means we are responding to an API call, the 
		//API caller must set the temp location...
		Objects.requireNonNull(fileContent);

		Validator.checkFile(fileContent, "content " + se.getRelPath());

		blob = new BlobFS(name, BLOB_TYPE_SOLDER_REPO, Integer.toString(sid), se.getCommitId(), mapInfo, tenantId,-1);
		BlobFileTransact bft = cg.beginFileTransact(blob);
		boolean fError = true;
		FileOutputStream fos = null;
		InputStream is = null;
		
		MessageDigest md = BlobFileTransact.tlMessageDigest.get();
		md.reset();
		
		try {
			is = new FileInputStream(fileContent);
			File fileOut = bft.getFile();
			fos = new FileOutputStream(fileOut);
			DigestOutputStream dos = new DigestOutputStream(fos, md);
			IOUtils.copy(is, dos);
			dos.close();

			byte[] digest = md.digest();
			String digestNew = PrintUtils.toHexString(digest);
			blob.setSizeAndDigest(fileContent.length(), digestNew);
			if (!CompareUtils.stringEquals(digestNew, se.getDigest())) {
				String stError = String.format("Write digest mismatch for %s. writeDigest=%s, prevCalc=%s",
						se.getRelPath(), digestNew, se.getDigest());
				LOG.info(stError);
				throw new SolderException(stError);
			}
			fError = false;
			bft.commit();
			se.setBlobFsId(blob.getId());
			return blob.getId();

		} finally {
			IOUtils.closeQuietly(fos, is);
			if (fError) {
				bft.abort();
			}
		}
	}
	
	public synchronized SCommit commitUpload(int commitId,SCommitInfo scommitInfo,File fileCommit) throws IOException {
		
		ContainerGroup cg = SolderMain.getSolderCg();
		Objects.requireNonNull(cg,()->SolderMain.SOLDER_CGREG_NAME+" registry setting");
		
		Objects.requireNonNull(scommitInfo,"scommit");
		Validator.checkFile(fileCommit, "fileCommit");
		if (scommitInfo.getRepoSeqId()!=sid) {
			throw new SolderException(String.format("CommitId %d; Unexpected repoId %d expect=%d",scommitInfo.getId(),scommitInfo.getRepoSeqId(),sid));
		}
		
		Validator.checkFile(fileCommit, "fileCommit");
		if (commitId <=0) {
			throw new SolderException("Invalid commitId "+commitId);
		}

		// Tip may have moved during UPLOAD_FILE*; refuse rather than parent onto a newer tip.
		requireExpectedTip(scommitInfo.getPrevId());
		
		SCommit scommitToCreate = new SCommit(this, scommitInfo.getCHash(),scommitInfo.getInfo(),commitId);
		
		
		
		
		Map<String, String> mapInfo = new HashMap<>();
		mapInfo.put("pid", SessionManager.getPid());
		mapInfo.put("commit_id", ""+commitId);
		
		CryptoScheme cs = CryptoScheme.getDefault();
		String name = cs.getTimeEncodedUUID();
		BlobFS blobCommit = new BlobFS(name, BLOB_TYPE_SOLDER_COMMIT, Integer.toString(sid), commitId, mapInfo,
				tenantId, -1);
		BlobFileTransact bft = cg.beginFileTransact(blobCommit);
		boolean fError = true;
		boolean fBlobCommitted = false;
		boolean fCommitInserted = false;
		FileOutputStream fos = null;
		InputStream is = null;
		MessageDigest md = BlobFileTransact.tlMessageDigest.get();
		md.reset();
		try {
			is = new FileInputStream(fileCommit);
			File fileOut = bft.getFile();
			fos = new FileOutputStream(fileOut);
			DigestOutputStream dos = new DigestOutputStream(fos, md);
			IOUtils.copy(is, dos);
			dos.close();
			byte[] digest = md.digest();
			String digestNew = PrintUtils.toHexString(digest);
			
			blobCommit.setSizeAndDigest(fileCommit.length(), digestNew);
			scommitToCreate.getInfo().put("digest", digestNew);
			bft.commit();
			fBlobCommitted = true;
			// Create SCommit.
			scommitToCreate.setBlobFsId(blobCommit.getId());
			scommitToCreate.insert();
			fCommitInserted = true;
			updateCommit(scommitToCreate);
			fError = false;
			return scommitToCreate;

		} finally {
			IOUtils.closeQuietly(fos, is);
			if (fError) {
				// Tip CAS / other failure after side effects: do not leave orphan commit or blob rows.
				if (fCommitInserted) {
					try {
						scommitToCreate.delete();
					} catch (Exception e) {
						LOG.warn(String.format("Failed to rollback scommit id=%d after commitUpload failure: %s",
								scommitToCreate.getId(), e.toString()));
					}
				}
				if (fBlobCommitted) {
					try {
						blobCommit.delete();
					} catch (Exception e) {
						LOG.warn(String.format("Failed to rollback commit blob id=%d after commitUpload failure: %s",
								blobCommit.getId(), e.toString()));
					}
				} else {
					bft.abort();
				}
			}
		}
		
		
	}
	
	public synchronized IVaultProvider getProvider(boolean fReadOnly) throws IOException {
		// Pick a Cache Directory...
		
		SyncLocalRepo syncCache = SyncLocalRepo.get(SyncLocalRepo.DEFAULT);
		File fileProv = syncCache.ensureSyncFolder(id);
		
		//Solder Sync On...
		SLocalRepo lrepo = new SLocalRepo(makeSRepoInfo(this), fileProv,true);
		//We want to make sure the lrepo has 
		if (commitId>0 && lrepo.getCommitId() != this.commitId ) {
			//Need to Sync...
			IRepoFileService rfs = ServerRepoFileService.get();
			RemoteRepoSync.repoCheckout(lrepo,null,rfs);
		}
		
		return new FileVaultProvider(fileProv.getAbsolutePath(), fReadOnly);
	}

	

	//Needed??
	public synchronized void repInit() throws IOException {
		

		SyncLocalRepo syncCache = SyncLocalRepo.get(SyncLocalRepo.DEFAULT);
		File fileLocalRepo = syncCache.ensureSyncFolder(id);
		try {
			IRepoFileService rfs = ServerRepoFileService.get();
			
			SLocalRepo lrepo = new SLocalRepo(makeSRepoInfo(this), fileLocalRepo,true);
			Objects.requireNonNull(rfs,"Repo File Service");
			
			String stScommitHash = scommit!=null?scommit.getCHash():"";
			int scommitId = scommit!=null?scommit.getId():-1;
			
			Event.log(SEvent.SRepoSync, sid, tenantId, (mb) -> {
				mb.put("fn", "repInit");
				mb.put("ldir", fileLocalRepo.getAbsolutePath());
				mb.put("table", "srepo");
				mb.put("id", id);
				mb.put("lrepoCommitId", lrepo.getCommitId());
				mb.put("lrepoCommitHash", lrepo.getCommitHash());
				mb.put("commitId", getCommitId());
				mb.put("scommitId", scommitId);
				mb.put("scommitHash", stScommitHash);
			});
			
			//For init is Fine, We want to overwrite even somethis there.
			
			if (getCommitId() > 0) {
				// Repository has commits..
				// Get the Latest..
				LOG.info(String.format("Repo %s has commit; latest=%d (date=%s)", getId(), getCommitId(),
						PrintUtils.print(getCommitDate())));
				RemoteRepoSync.repoCheckout(lrepo,null,rfs);
			} else {
				LOG.info(String.format("Repo %s has no commits. Nothing to do", getId()));
			}
			
			
		} catch (Exception e) {
			Event.log(SEvent.SRepoSyncError, sid, tenantId, (mb) -> {
				mb.put("fn", "repInit");
				mb.put("id", id);
				mb.put("ldir", fileLocalRepo.getAbsolutePath());
				mb.put("error", PrintUtils.getStackTrace(e));
			});
			throw SolderException.rethrow(e);
		}
	}

	// Auto create with random id.
	public static SRepo ensureRepo(String schemaName, int aoId, String tag) throws IOException {

		schemaName = Validator.require(schemaName, "schemaName", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
		tag = Validator.require(tag, "tag", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
		ContainerGroup cg = SolderMain.getSolderCg();
		Objects.requireNonNull(cg, "SolderCg not set!");

		SRepo repo = getRepoByUnique(Tenant.ROOT_ID, schemaName, aoId);
		if (repo == null) {
			// try to create one...
			String stRandom = CryptoScheme.getDefault().getUUID();
			String repoId = String.format("%s_%s", schemaName, stRandom.substring(0, 8));
			try {
				repo = ensureSRepo(repoId, schemaName, Tenant.ROOT_ID, aoId, tag);
			} catch (IOException e) {
				// Race?
				try {
					repo = getRepoByUnique(Tenant.ROOT_ID, schemaName, aoId);
					if (repo == null) {
						LOG.error(String.format("Unable to recover create error ex=%s", PrintUtils.getStackTrace(e)));
						throw SolderException.rethrow(e);
					}
				} catch (Exception e2) {
					LOG.error(String.format("Ignore second error and throw first error; e2=%s",
							PrintUtils.getStackTrace(e2)));
					throw SolderException.rethrow(e);
				}
			}
		} else {
			if (!StringUtils.isEmpty(tag) && !CompareUtils.stringEquals(repo.getTag(), tag)) {
				LOG.debug(String.format("ensureSRepo Found existing Repo %s Update tag to %s", repo.getId(), tag));
				repo.updateChange(tag, null);
			}
		}

		return repo;
	}

	// Auto create with specific id. --
	public static SRepo ensureSRepo(String id, String tschema, int tenantId, int aoId, String tag)
			throws IOException {
		LOG.trace(String.format("SolderVaultFactory ensureSRepo  %s tschema=%s tag=%s tenantId=%d aoId=%d", id,
				tschema, tag, tenantId, aoId));
		id = Validator.require(id, "repo id", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
		tschema = Validator.require(tschema, "tschema", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
		SRepo repo = getRepoById(id);
		if (repo != null) {
			// Make sure schema matches
			if (repo.getTenantId() != tenantId) {
				throw new RestException("Id already taken by another tenant. given=" + id);
			}
			if (!CompareUtils.stringEquals(tschema, repo.getTSchema())) {
				throw new RestException("A previous repo with a different schema " + repo.getTSchema()
						+ " exist! id=" + repo.getId() + ", expected tschema " + tschema);
			}

			LOG.trace(String.format("SolderVaultFactory ensureSRepo Found existing Repo %s tag=%s currentTag=%s ", id,
					tag, repo.getTag()));

			if (!StringUtils.isEmpty(tag) && !CompareUtils.stringEquals(repo.getTag(), tag)) {
				LOG.trace(String.format("SolderVaultFactory ensureSRepo Found existing Repo %s Update tag to %s", id,
						tag));
				repo.updateChange(tag, null);
			}
			return repo;
		} else {

			repo = new SRepo(id, tschema, tenantId, aoId, tag, "Commits");
			TVault tvault = TVault.open(SolderVaultFactory.TYPE, repo.getId(), Mode.CREATE, null);
			tvault.close();
			LOG.info(String.format("GitSync %s newly created Tault ", repo.getId()));
			SolderVaultFactory svf = (SolderVaultFactory) TVault.getFactory(SolderVaultFactory.TYPE);
			svf.repoGitPush(repo.getId());
			return repo;
		}

	}

	public static SRepo getRepoById(String id) throws IOException {
		String idFinal = Validator.require(id, "id", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);

		String key = CacheHelper.getKey(CacheHelper.KEY_ID, idFinal);
		return cacheRepo.getStoreIfAbsent(key, () -> selectRepoById(idFinal, null), (srepo) -> srepo.cacheKeys());

	}

	public static SRepo getRepoBySeqId(int sid) throws IOException {

		String key = CacheHelper.getKey(SRepo.KEY_SID, sid);
		return cacheRepo.getStoreIfAbsent(key, () -> selectRepoBySeqId(sid, null), (srepo) -> srepo.cacheKeys());

	}

	public static SRepo getRepoByUnique(int tenantId, String schemaName, int aoId) throws IOException {
		String schemaNameFinal = Validator.require(schemaName, "schema name", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);

		String key = CacheHelper.getKey(CacheHelper.KEY_TENANT_TYPE_NAME, tenantId, schemaName, String.valueOf(aoId));
		return cacheRepo.getStoreIfAbsent(key, () -> selectByUnique(tenantId, schemaNameFinal, aoId),
				(srepo) -> srepo.cacheKeys());

	}

	static SRepo selectRepoById(String id, SRepo srepo) throws IOException {
		TReference<SRepo> tref = new TReference<>();
		SRepo srepoFinal = srepo != null ? srepo : new SRepo();
		SQLTm.get().select(repQ.qRepoSelId, (encoder) -> {
			encoder.writeString("id", id);
		}, (decoder) -> {
			if (decoder.next()) {
				srepoFinal.deserialize(decoder);
				tref.set(srepoFinal);
			}
		}, null);
		return tref.get();
	}

	static SRepo selectRepoBySeqId(int sid, SRepo srepo) throws IOException {
		TReference<SRepo> tref = new TReference<>();
		SRepo srepoFinal = srepo != null ? srepo : new SRepo();
		SQLTm.get().select(repQ.qRepoSelSid, (encoder) -> {
			encoder.writeInt("sid", sid);
		}, (decoder) -> {
			if (decoder.next()) {
				srepoFinal.deserialize(decoder);
				tref.set(srepoFinal);
			}
		}, null);
		return tref.get();
	}

	static SRepo selectByUnique(int tenantId, String schemaName, int aoId) throws IOException {
		TReference<SRepo> tref = new TReference<>();

		SQLTm.get().select(repQ.qRepoSelUnique, (encoder) -> {
			encoder.writeInt("tenant_id", tenantId);
			encoder.writeString("tschema", schemaName);
			encoder.writeInt("ao_id", aoId);
		}, (decoder) -> {
			if (decoder.next()) {
				SRepo srepo = new SRepo();
				srepo.deserialize(decoder);
				tref.set(srepo);
			}
		}, null);
		return tref.get();
	}

	public static List<SRepo> selectByTenant(int tenantId) throws IOException {

		List<SRepo> list = new ArrayList<>();

		SQLTm.get().select(repQ.qRepoSelTenant, (encoder) -> {
			encoder.writeInt("tenant_id", tenantId);
			encoder.writeBoolean("deleted", false);
		}, (decoder) -> {

			while (decoder.next()) {
				SRepo srepo = new SRepo();
				srepo.deserialize(decoder);
				list.add(srepo);
			}
		}, null);
		return list;
	}

	/**
	 * Use only simple * pattern.. Both * and ? are coverted to % in sql...
	 *
	 */

	private static SQLQuery getRepoSearch(boolean fIdPattern, boolean fSchemaPattern, boolean fTagFilter,
			boolean fNonDeletedRepo) throws IOException {
		SQLQuery q = repQ.qRepoSelSid;

		String stInitial = fNonDeletedRepo ? "tenant_id,deleted" : "tenant_id";

		SQLQuery qRepoSearch = DriverUtil.createSelectQuery(q.getDBName(), q.getType(), repQ.tsRepo, stInitial,
				"ByTenantSearch", (sb) -> {
					if (fIdPattern) {
						sb.append(" AND id like ?");
					}
					if (fSchemaPattern) {
						sb.append(" AND tschema like ?");
					}
					if (fTagFilter) {
						sb.append(" AND tag=?");
					}
				}, null);
		return qRepoSearch;
	}

	public static List<SRepo> searchRepo(int tenantId, String idWild, String schemaWild, String tagFilter)
			throws IOException {

		boolean fIdWild = !StringUtils.isEmpty(idWild);
		boolean fSchemaWild = !StringUtils.isEmpty(schemaWild);
		boolean fTagFilter = !StringUtils.isEmpty(tagFilter);

		if (!fIdWild && !fSchemaWild && !fTagFilter) {
			// You want everything for tenantId...
			return selectByTenant(tenantId);
		}

		SQLQuery qRepoSearch = getRepoSearch(fIdWild, fSchemaWild, fTagFilter, true);

		List<SRepo> list = new ArrayList<>();
		SQLTm.get().select(qRepoSearch, (encoder) -> {
			encoder.writeInt("tenant_id", tenantId);
			encoder.writeBoolean("deleted", false);
			if (fIdWild) {
				encoder.writeString("id", SQLUtil.replaceWild(idWild));
			}
			if (fSchemaWild) {
				encoder.writeString("tschema", SQLUtil.replaceWild(schemaWild));
			}
			if (fTagFilter) {
				encoder.writeString("tag", tagFilter);
			}
		}, (decoder) -> {

			while (decoder.next()) {
				SRepo srepo = new SRepo();
				srepo.deserialize(decoder);
				list.add(srepo);
			}
		}, null);
		return list;
	}

	public static List<SRepo> getDeletedRepo(int tenantId, String id) throws IOException {
		String idPattern = Validator.require(id, "id", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);

		SQLQuery qRepoSearch = getRepoSearch(true, false, false, false);

		List<SRepo> list = new ArrayList<>();
		SQLTm.get().select(qRepoSearch, (encoder) -> {
			encoder.writeInt("tenant_id", tenantId);
			encoder.writeString("id", SQLUtil.replaceWild(idPattern + "_*"));

		}, (decoder) -> {

			while (decoder.next()) {
				SRepo srepo = new SRepo();
				srepo.deserialize(decoder);
				list.add(srepo);
			}
		}, null);
		return list;
	}
}
