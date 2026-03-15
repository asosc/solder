package org.solder.vsync;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nimbo.blobs.BlobFS;
import org.solder.core.SAudit;
import org.solder.core.SEvent;
import org.solder.core.SolderException;
import org.solder.rest.client.RemoteRepoSync;
import org.solder.rest.client.RemoteRepoSync.IRepoFileService;
import org.solder.rest.client.RemoteRepoSync.SLocalRepo;
import org.solder.rest.client.SCommitInfo;
import org.solder.rest.client.SRepoInfo;

import com.aura.crypto.CryptoScheme;
import com.beech.bfs.Mode;
import com.beech.store.FileVaultProvider;
import com.beech.store.IVaultFactory;
import com.beech.store.IVaultProvider;
import com.beech.store.TVault;
import com.ee.rest.RestException;
import com.ee.session.SQLTm;
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
import com.lnk.lucene.RunOnce;

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
		
		SRepo srepo = getRepoById(id);
		Objects.requireNonNull(srepo, "srepo " + id);
		prov = new SolderVaultProvider(srepo,false);
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
		
		SolderVaultProvider(SRepo repo,boolean fReadOnly) throws IOException{
			this.repo=repo;
			syncCache = SyncLocalRepo.get(SyncLocalRepo.DEFAULT);
			fileProv = syncCache.ensureSyncFolder(repo.getId());
			//Solder Sync On...
			lrepo = new SLocalRepo(repo, fileProv,true);
			//We want to make sure the lrepo has 
			if (repo.getCommitId()>0 && lrepo.getCommitId() != repo.getCommitId() ) {
				//Need to Sync...
				IRepoFileService rfs = ServerRepoFileService.get();
				RemoteRepoSync.repoCheckout(lrepo,rfs);
			}
			fvp = new FileVaultProvider(fileProv.getAbsolutePath(), fReadOnly);
		}
		
		public void repoGitPush() throws IOException{
			IRepoFileService rfs = ServerRepoFileService.get();
			RemoteRepoSync.repCommit(lrepo, fileProv, (props)->{
				props.put("message","SolderVaultProvider");
			},rfs);
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
			return false;
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
		
	}

	static final String SREPO_TABLE = "srepo";
	static final String SREPO_SEQ = "srepo_seq";
	static final String SCOMMIT_TABLE = "scommit";
	static final String SCOMMIT_SEQ = "scommit_seq";

	static AtomicBoolean s_fInit = new AtomicBoolean(false);
	static RepQueries repQ = null;
	static Cache<SRepo> cacheRepo = null;

	public static void init(SQLDatabase db) throws IOException {
		SQLDatabase dbFinal = Objects.requireNonNull(db, "db");
		RunOnce.ensure(s_fInit, () -> {
			repQ = new RepQueries(dbFinal.getName(), dbFinal.getType());
			SolderSentryProvider.init();
		});
	}

	public static class RepQueries {

		SQLTableSchema tsRepo, tsCommit;

		SQLQuery qRepoIns, qRepoSelId,qRepoSelSid,qRepoSelUnique,qRepoSelTenant, qRepoUpdCommit, qRepoUpdChange,qRepoUpdDel, qRepoDelOne,qRepoSeq;

		SQLQuery qCommitIns, qCommitSelId, qCommitSelRepo, qCommitDelOne, qCommitSeq;

		RepQueries(String dbName, DBType dbType) throws IOException {

			// (name,fieldType(canonicalName),[flags(0,1),nSpit])

			tsRepo = new SQLTableSchema(SREPO_TABLE);
			tsRepo.parseAndAdd(new String[] { "sid,int,1","id,string,1", "tschema,string(128),1", "tenant_id,int,1",
					"ao_id,int,1","deleted,boolean,1", "commit_dir,string,0", "ext_keep,string,1", "commit_id,int,1", "commit_date,date,1",
					"change_date,date,1", "create_date,date,1", "last_update,date,1" });

			String stPrimaryKey = "id";
			String[] aUnique = new String[] { "tenant_id,tschema,ao_id","sid" };
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
			qRepoSelUnique = DriverUtil.createSelectQuery(dbName, dbType, tsRepo, "tenant_id,tschema,ao_id", "ByUnique");
			qRepoSelTenant = DriverUtil.createSelectQuery(dbName, dbType, tsRepo, "tenant_id,deleted", "ByTenant");
			
			qRepoUpdCommit = DriverUtil.createUpdateQuery(dbName, dbType, tsRepo, "commit_id,commit_date,last_update",
					"sid,commit_id", "Commit");
			qRepoUpdChange = DriverUtil.createUpdateQuery(dbName, dbType, tsRepo, "change_date,last_update", "sid",
					"Change");
			
			qRepoUpdDel = DriverUtil.createUpdateQuery(dbName, dbType, tsRepo, "id,tschema,deleted,last_update", "sid,id",
					"Del");
			
			qRepoDelOne = DriverUtil.createDeleteQuery(dbName, dbType, tsRepo, "sid", "One");
			SQLQuery.addToMap(qRepoIns, qRepoSelId,qRepoSelUnique, qRepoUpdCommit, qRepoUpdChange, qRepoDelOne);

			cacheRepo = BackgroundTask.get().createCache(SREPO_TABLE, true);

			tsCommit = new SQLTableSchema(SCOMMIT_TABLE);
			tsCommit.parseAndAdd(new String[] { "id,int,1", "repo_id,string(48),1", "chash,string(128),1",
					"prev_id,int,1","prev_chash,string(128),1",
					"tenant_id,int,1", "blob_fsid,long,1", "create_date,date,1", "info,string,0,3" });

			stPrimaryKey = "id";
			aUnique = new String[] { "repo_id,prev_id,chash" };
			aIndex = null;

			tsCommit.setCreateScriptParams(stPrimaryKey, aUnique, aIndex, Tenant.FILE_GROUP, SCOMMIT_SEQ);
			tsCommit.setReadOnly();
			SQLTableSchema.register(tsCommit);

			MSSQLUtil.getCreateTableScript(tsCommit);
			MSSQLUtil.getCreateSequenceScript(SCOMMIT_SEQ);

			qCommitSeq = DriverUtil.createSequenceQuery(dbName, dbType, tsCommit, SCOMMIT_SEQ);
			qCommitIns = DriverUtil.createInsertQuery(dbName, dbType, tsCommit);
			qCommitSelId = DriverUtil.createSelectQuery(dbName, dbType, tsCommit, "id", "ById");
			qCommitSelRepo = DriverUtil.createSelectQuery(dbName, dbType, tsCommit, "repo_id", "ByRepo");
			qCommitDelOne = DriverUtil.createDeleteQuery(dbName, dbType, tsCommit, "id", "One");
			SQLQuery.addToMap(qCommitIns, qCommitSelRepo, qCommitDelOne, qCommitSeq);
		}
	}
	
	static int generateSRepoId() throws IOException {
		return (int) SQLTm.get().nextSequenceId(repQ.qRepoSeq);
	}

	static int generateCommitId() throws IOException {
		return (int) SQLTm.get().nextSequenceId(repQ.qCommitSeq);
	}

	public static class SCommit extends SCommitInfo implements Comparable<SCommitInfo> {
		

		public SCommit() {
		}

		public SCommit(SRepo repo, String chash, Map<String, String> mapInfo, int commitId) throws IOException {
			// Generated from sequence.
			if (commitId <= 0 || commitId <= repo.getCommitId()) {
				throw new SolderException("Invalid commitId " + commitId);
			}
			this.id = commitId;
			Objects.requireNonNull(repo, "repo");
			this.repoId = repo.getId();
			this.chash = Validator.require(chash, "chash", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
			this.tenantId = repo.getTenantId();
			
			if (repo.getCommitId()<=0) {
				idPrev=0;
				chashPrev = "cafe00";
			} else {
				idPrev = repo.getCommitId();
				chashPrev = repo.getLatestCommit().getCHash();
			}

			dateCreate = new Date();
			if (mapInfo == null) {
				mapInfo = new LinkedHashMap<>();
			}
			this.mapInfo = mapInfo;
		}

	
		
		
		public BlobFS getBlobFs() throws IOException{
			if (blobFsId<=0) {
				//Auto look up by owner??? May be...
				return null;
			}
			BlobFS blobFs = BlobFS.getById(blobFsId);
			Objects.requireNonNull(blobFs,"blob fs "+blobFsId);
			return blobFs;
		}

		

		void insert() throws IOException {
			// We get the transactions sqlTm.
			if (id < 0) {
				throw new SolderException("Commit Id not set");
			}
			SQLTm.get().executeOne(repQ.qCommitIns, this::serialize, null);
			Event.log(SEvent.SCommitCreate, id, tenantId, (mb) -> {
				mb.put("repo_id", repoId);
				mb.put("chash", chash);
			});
		}

		public void delete() throws IOException {
			int i = SQLTm.get().executeOne(repQ.qCommitDelOne, (encoder) -> {
				encoder.writeInt("id", id);
			}, null);

			if (i < 1) {
				Event.log(EEvent.DbDeleteFail, id, tenantId, (mb) -> {
					mb.put("table", "srepo");
					mb.put("id", id);
				});
			} else {
				Event.log(SEvent.SCommitDelete, id, tenantId, (mb) -> {
					mb.put("repo_id", repoId);
					mb.put("chash", chash);
				});
			}

		}
	}

	static SCommit selectCommitById(int id) throws IOException {
		TReference<SCommit> tref = new TReference<>();
		SQLTm.get().select(repQ.qCommitSelId, (encoder) -> {
			encoder.writeInt("id", id);
		}, (decoder) -> {
			if (decoder.next()) {
				SCommit scommit = new SCommit();
				scommit.deserialize(decoder);
				tref.set(scommit);
			}
		}, null);
		return tref.get();
	}

	static List<SCommit> selectCommitByRepo(String repoId) throws IOException {
		String repoIdFinal = Validator.require(repoId, "repo_id", Rules.NO_NULL_EMPTY, Rules.TRIM);
		List<SCommit> list = new ArrayList<>();
		SQLTm.get().select(repQ.qCommitSelRepo, (encoder) -> {
			encoder.writeString("repo_id", repoIdFinal);
		}, (decoder) -> {
			while (decoder.next()) {
				SCommit scommit = new SCommit();
				scommit.deserialize(decoder);
				list.add(scommit);
			}
		}, null);
		return list;
	}
	
	
	public static SRepo ensureSRepo(String id, String schemaName, int tenantId, int aoId) throws IOException {
		id = Validator.require(id, "repo id",Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
		schemaName= Validator.require(schemaName, "schema name",Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
		SRepo repo = SolderVaultFactory.getRepoById(id);
		if (repo != null) {
			//Make sure schema matches
			if (repo.getTenantId() != tenantId) {
				throw new RestException("Id already taken by another tenant. given="+id);
			}
			if (!CompareUtils.stringEquals(schemaName,repo.getSchemaName())) {
				throw new RestException("A previous repo with a different schema "+repo.getSchemaName()+" exist! id="+repo.getId()+", expected schema "+schemaName);
			}
			return repo;
		} else {
		
			repo = new SRepo(id, schemaName, tenantId, aoId,"Commits",null);
			TVault tvault = TVault.open(SolderVaultFactory.TYPE, repo.getId(),Mode.CREATE, null);
			tvault.close();
			LOG.info(String.format("GitSync %s newly created Tault ",repo.getId()));
			SolderVaultFactory svf = (SolderVaultFactory)TVault.getFactory(SolderVaultFactory.TYPE);
			svf.repoGitPush(repo.getId());
			return repo;
		}
		
		
	}

	public static class SRepo extends SRepoInfo  {
		

		public SRepo() {
			super();
		}

		public SRepo(String id, String schemaName, int tenantId, int aoId, String commitDir, String[] aExtensions)
				throws IOException {
			super();
			this.id = Validator.require(id, "id", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
			this.schemaName = Validator.require(schemaName, "schema name", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
			
			Validator.require(id.length(),2,48,"id Length", Rules.MIN_MAX);
			Validator.require(schemaName.length(),1,32,"schema name Length", Rules.MIN_MAX);
			


			if (commitDir == null) {
				commitDir = "";
			} else {
				commitDir = commitDir.trim();
			}
			this.commitDir = commitDir;

			if (aExtensions != null) {
				for (int i = 0; i < aExtensions.length; i++) {
					aExtensions[i] = aExtensions[i].trim().toLowerCase();
				}
			}

			this.aExtension = aExtensions;

			this.tenantId = tenantId;
			this.aoId = aoId;
			commitId = 0;
			dateCreate = new Date();
			dateUpdate = dateCreate;
			// Just put date rep some number less than create..
			dateCommit = new Date(dateCreate.getTime() - TimeUnit.DAYS.toMillis(30));
			dateChange = dateCreate;
			// Create scache before you insert..
			getProvider(false);
			this.sid=generateSRepoId();
			insert();
		}
		
		//We keep it overridden because this is used both db and other serializer.

		/*
		public void serialize(Encoder encoder) throws IOException {
			encoder.writeInt("sid", sid);
			encoder.writeString("id", id);
			encoder.writeString("tschema", schemaName);
			encoder.writeInt("tenant_id", tenantId);
			encoder.writeInt("ao_id", aoId);
			encoder.writeBoolean("deleted", fDeleted);
			encoder.writeString("commit_dir", commitDir);
			encoder.writeStringArray("ext_keep", aExtension);
			encoder.writeInt("commit_id", commitId);
			encoder.writeDate("commit_date", dateCommit);
			encoder.writeDate("change_date", dateChange);
			encoder.writeDate("create_date", dateCreate);
			encoder.writeDate("last_update", dateUpdate);
		}

		public void deserialize(Decoder decoder) throws IOException {
			sid = decoder.readInt("sid");
			id = decoder.readString("id");
			schemaName = decoder.readString("tschema");
			tenantId = decoder.readInt("tenant_id");
			aoId = decoder.readInt("ao_id");
			fDeleted = decoder.readBoolean("deleted");
			commitDir = decoder.readString("commit_dir");
			aExtension = decoder.readStringArray("ext_keep");
			commitId = decoder.readInt("commit_id");
			dateCommit = decoder.readDate("commit_date");
			dateChange = decoder.readDate("change_date");
			dateCreate = decoder.readDate("create_date");
			dateUpdate = decoder.readDate("last_update");
		}
		*/
		static final String KEY_SID="sid";

		public String[] cacheKeys() {
			return new String[] { CacheHelper.getKey(CacheHelper.KEY_ID, id),
					CacheHelper.getKey(CacheHelper.KEY_TENANT_TYPE_NAME, tenantId, schemaName, String.valueOf(aoId)),
					CacheHelper.getKey(KEY_SID, sid)};
		}
		
		public synchronized void refresh() throws IOException {
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

		public synchronized String getSchemaName() {
			return schemaName;
		}

		public synchronized int getTenantId() {
			return tenantId;
		}

		public synchronized int getAoId() {
			return aoId;
		}

		public synchronized String getCommitDir() {
			return commitDir;
		}

		public synchronized String[] getExtensionToKeep() {
			return aExtension;
		}

		public synchronized int getCommitId() {
			return commitId;
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
		
		public int generateNewCommitId() throws IOException {
			return SolderVaultFactory.generateCommitId();
		}
		
		
		SyncLocalRepo syncCache = null;

		// For now copy everything so we get testing..
		// We can optimize by not copying when the file is locally available..

		public synchronized IVaultProvider getProvider(boolean fReadOnly) throws IOException {
			// Pick a Cache Directory...
			
			SyncLocalRepo syncCache = SyncLocalRepo.get(SyncLocalRepo.DEFAULT);
			File fileProv = syncCache.ensureSyncFolder(id);
			
			//Solder Sync On...
			SLocalRepo lrepo = new SLocalRepo(this, fileProv,true);
			//We want to make sure the lrepo has 
			if (commitId>0 && lrepo.getCommitId() != this.commitId ) {
				//Need to Sync...
				IRepoFileService rfs = ServerRepoFileService.get();
				RemoteRepoSync.repoCheckout(lrepo,rfs);
			}
			
			return new FileVaultProvider(fileProv.getAbsolutePath(), fReadOnly);
		}

		void insert() throws IOException {
			// We get the transactions sqlTm.
			if (this.sid<=0) {
				throw new SolderException("SRepo Id not set");
			}

			SQLTm.get().executeOne(repQ.qRepoIns, this::serialize, null);

			// Add to the cache
			cacheRepo.store(this, this::cacheKeys);
			Audit.audit(SAudit.SRepo_Create, sid, -1, tenantId, (cmb) -> {
				cmb.put("id", id);
				cmb.put("schema", schemaName);
			});
		}

		public synchronized void updateChange(Date dateChange) throws IOException {

			Date dateChangeFinal = Objects.requireNonNull(dateChange);
			Date dateUpdateNew = new Date();

			int i = SQLTm.get().executeOne(repQ.qRepoUpdChange, (encoder) -> {

				encoder.writeDate("change_date", dateChange);
				encoder.writeDate("last_update", dateUpdateNew);

				// Where clause come
				encoder.writeInt("sid", sid);

			}, null);
			if (i == 1) {
				this.dateChange = dateChangeFinal;
				this.dateUpdate = dateUpdateNew;

				Audit.audit(SAudit.SRepo_Update, sid, -1, tenantId, (cmb) -> {
					cmb.put("id", id);
					cmb.put("op", "change_date");
					cmb.putIfChanged("change_date", dateChangeFinal, dateChange);
				});

			} else {
				Event.log(SEvent.DbUpdateFail, sid, tenantId, (mb) -> {
					mb.put("table", "srepo");
					mb.put("id", id);
				});
			}

		}
		
		
		public synchronized void updateDelete() throws IOException {
			if (fDeleted) {
				throw new SolderException("Repo "+id+" already deleted!");
			}
			
			String stUnique = CryptoScheme.getDefault().getUUID().substring(1,5);
			DateFormat df = new SimpleDateFormat("yyMMddHH");
			
			String suffix = "_"+stUnique+"_"+df.format(new Date());
			
			String idDel = id+suffix;
			String schemaDel = schemaName+suffix;

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
					cmb.putIfChanged("id",this.id,idDel);
					cmb.putIfChanged("tschema", this.schemaName,schemaDel);
					cmb.putIfChanged("deleted", this.fDeleted,fDeleteNow);
					
				});
				
				cacheRepo.remove(CacheHelper.getKey(KEY_SID, sid));
				
				this.id = idDel;
				this.schemaName = schemaDel;
				this.fDeleted=fDeleteNow;
				this.dateUpdate = dateUpdateNew;

			} else {
				Event.log(SEvent.DbUpdateFail, sid, tenantId, (mb) -> {
					mb.put("table", "srepo");
					mb.put("op", "update_del");
					mb.put("id", id);
				});
			}

		}
		

		public synchronized void updateCommit(SCommit commit) throws IOException {

			Objects.requireNonNull(commit);
			
			if (this.commitId>0 && commitId != commit.getPrevId()) {
				throw new SolderException("CommitId mismatch; current="+commitId+"; given SCommit prev="+commit.getPrevId());
			}
			
			int commitIdNew = commit.getId();
			Date dateCommitNew = commit.getCreateDate();
			if (commitIdNew <= this.commitId) {
				throw new SolderException("Commit id sequencing error; new commit " + commitIdNew
						+ " must be newer than current commit " + commitId);
			}

			Date dateUpdateNew = new Date();

			int i = SQLTm.get().executeOne(repQ.qRepoUpdCommit, (encoder) -> {
				encoder.writeInt("commit_id", commitIdNew);
				encoder.writeDate("commit_date", dateCommitNew);
				encoder.writeDate("last_update", dateUpdateNew);

				// Where clause come
				encoder.writeInt("sid", sid);
				encoder.writeInt("commit_id", commitId);
				
			}, null);
			if (i == 1) {
				Audit.audit(SAudit.SRepo_Update, sid, -1, tenantId, (cmb) -> {
					cmb.put("id", id);
					cmb.put("op", "rep_info");
					cmb.putIfChanged("commit_id", commitIdNew, commitId);
					cmb.putIfChanged("commit_date", dateCommitNew, dateCommit);

				});
				this.commitId = commitIdNew;
				this.dateCommit = dateCommitNew;
				this.dateUpdate = dateUpdateNew;

			} else {
				Event.log(SEvent.DbUpdateFail, sid, tenantId, (mb) -> {
					mb.put("table", "srepo");
					mb.put("id", id);
					mb.put("commit_id", commitIdNew);
				});
			}

		}

		//To be used by PURGE When it is implemented..
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
					cmb.put("schema", schemaName);
				});
			}

		}

		public synchronized void repInit() throws IOException {
			Event.log(SEvent.SRepoSync, sid, tenantId, (mb) -> {
				mb.put("table", "srepo");
				mb.put("id", id);
			});

			SyncLocalRepo syncCache = SyncLocalRepo.get(SyncLocalRepo.DEFAULT);
			File fileLocalRepo = syncCache.ensureSyncFolder(id);
			try {
				IRepoFileService rfs = ServerRepoFileService.get();
				
				SLocalRepo lrepo = new SLocalRepo(this, fileLocalRepo,true);
				Objects.requireNonNull(rfs,"Repo File Service");
				if (getCommitId() > 0) {
					// Repository has commits..
					// Get the Latest..
					LOG.info(String.format("Repo %s has commit; latest=%d (date=%s)", getId(), getCommitId(),
							PrintUtils.print(getCommitDate())));
					RemoteRepoSync.repoCheckout(lrepo,rfs);
				} else {
					LOG.info(String.format("Repo %s has no commits. Nothing to do", getId()));
				}
				
				
			} catch (Exception e) {
				Event.log(SEvent.SRepoSyncError, sid, tenantId, (mb) -> {
					mb.put("table", "srepo`");
					mb.put("id", id);
					mb.put("error", PrintUtils.getStackTrace(e));
				});
				throw SolderException.rethrow(e);
			}
		}

		SCommit scommit;

		public synchronized SCommit getLatestCommit() throws IOException {
			
			refresh();
			
			if (commitId <= 0) {
				return null;
			}
			
			if (scommit != null) {
				if (scommit.getId() != this.commitId) {
					scommit = null;
				}
			}
			if (scommit == null) {
				
				scommit = selectCommitById(commitId);
			}
			Objects.requireNonNull(scommit, "scommit " + commitId);
			return scommit;
		}

		public List<SCommit> getAllCommit() throws IOException {
			List<SCommit> listCommits = selectCommitByRepo(this.id);
			Collections.sort(listCommits);
			return listCommits;
		}
	}

	public static SRepo getRepoById(String id) throws IOException {
		String idFinal = Validator.require(id, "id", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);

		String key = CacheHelper.getKey(CacheHelper.KEY_ID, idFinal);
		return cacheRepo.getStoreIfAbsent(key, () -> selectRepoById(idFinal,null), (srepo) -> srepo.cacheKeys());

	}
	
	public static SRepo getRepoBySeqId(int sid) throws IOException {
		
		String key = CacheHelper.getKey(SRepo.KEY_SID, sid);
		return cacheRepo.getStoreIfAbsent(key, () -> selectRepoBySeqId(sid,null), (srepo) -> srepo.cacheKeys());

	}
	
	
	public static SRepo getRepoByUnique(int tenantId,String schemaName,int aoId) throws IOException {
		String schemaNameFinal = Validator.require(schemaName, "schema name", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);

		String key = CacheHelper.getKey(CacheHelper.KEY_TENANT_TYPE_NAME, tenantId, schemaName,String.valueOf(aoId));
		return cacheRepo.getStoreIfAbsent(key, () -> selectByUnique(tenantId,schemaNameFinal,aoId), (srepo) -> srepo.cacheKeys());

	}

	static SRepo selectRepoById(String id,SRepo srepo) throws IOException {
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
	
	static SRepo selectRepoBySeqId(int sid,SRepo srepo) throws IOException {
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
	
	static SRepo selectByUnique(int tenantId,String schemaName,int aoId) throws IOException {
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
	
	private static SQLQuery getRepoSearch(boolean fIdPattern,boolean fSchemaPattern,boolean fNonDeletedRepo) throws IOException {
		SQLQuery q=repQ.qRepoSelSid;
		
		String stInitial = fNonDeletedRepo?"tenant_id,deleted":"tenant_id";
		
		SQLQuery qRepoSearch = DriverUtil.createSelectQuery(q.getDBName(), q.getType(), repQ.tsRepo,
				stInitial, "ByTenantSearch",(sb)->{
					if (fIdPattern) {
						sb.append(" AND id like ?");
					}
					if (fSchemaPattern) {
						sb.append(" AND tschema like ?");
					}
				},null);
		return qRepoSearch;
	}
	
	public static List<SRepo> searchRepo(int tenantId,String idPattern, String schemaPattern) throws IOException {
		
		boolean fIdPattern = !StringUtils.isEmpty(idPattern);
		boolean fSchemaPattern = !StringUtils.isEmpty(schemaPattern);
		
		if (!fIdPattern && !fSchemaPattern) {
			//You want everything for tenantId...
			return selectByTenant(tenantId);
		}
		
		SQLQuery qRepoSearch = getRepoSearch(fIdPattern,fSchemaPattern,true); 
		
		List<SRepo> list = new ArrayList<>();
		SQLTm.get().select(qRepoSearch, (encoder) -> {
			encoder.writeInt("tenant_id", tenantId);
			encoder.writeBoolean("deleted", false);
			if (fIdPattern) {
				encoder.writeString("id", SQLUtil.replaceWild(idPattern));
			}
			if (fSchemaPattern) {
				encoder.writeString("tschema", SQLUtil.replaceWild(schemaPattern));
			}
		}, (decoder) -> {
			
			while (decoder.next()) {
				SRepo srepo = new SRepo();
				srepo.deserialize(decoder);
				list.add(srepo);
			}
		},null);
		return list;
	}
	
	public static List<SRepo> getDeletedRepo(int tenantId,String id) throws IOException {
		String idPattern = Validator.require(id, "id", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
		
		SQLQuery qRepoSearch = getRepoSearch(true,false,false); 
		
		List<SRepo> list = new ArrayList<>();
		SQLTm.get().select(qRepoSearch, (encoder) -> {
			encoder.writeInt("tenant_id", tenantId);
			encoder.writeString("id", SQLUtil.replaceWild(idPattern+"_*"));
			
		}, (decoder) -> {
			
			while (decoder.next()) {
				SRepo srepo = new SRepo();
				srepo.deserialize(decoder);
				list.add(srepo);
			}
		},null);
		return list;
	}

}
