package org.solder.vsync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.core.SAudit;
import org.solder.core.SEvent;
import org.solder.core.SolderException;

import com.beech.store.FileVaultProvider;
import com.beech.store.IVaultFactory;
import com.beech.store.IVaultProvider;
import com.beech.store.TVault;
import com.ee.session.SQLTm;
import com.ee.session.db.Audit;
import com.ee.session.db.BackgroundTask;
import com.ee.session.db.EEvent;
import com.ee.session.db.Event;
import com.ee.session.db.Tenant;
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
import com.lnk.lucene.RunOnce;
import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;

public class SolderVaultFactory implements IVaultFactory {

	private static Log LOG = LogFactory.getLog(SolderVaultFactory.class.getName());

	public static final String TYPE = "solder_repo";
	

	public SolderVaultFactory() {
		TVault.registerFactory(this);
	}

	public String getType() {
		return TYPE;
	}

	public IVaultProvider getProvider(String stFactoryParams, boolean fReadOnly) throws IOException {
		String id = stFactoryParams;
		SRepo srepo = getById(id);
		Objects.requireNonNull(srepo, "srepo " + id);
		return srepo.getProvider(fReadOnly);
	}

	static final String SREPO_TABLE = "srepo";
	static final String SCOMMIT_TABLE = "scommit";
	static final String SCOMMIT_SEQ = "scommit_seq";

	static AtomicBoolean s_fInit = new AtomicBoolean(false);
	static RepQueries reqQ = null;
	static Cache<SRepo> cacheRepo = null;

	public static void init(SQLDatabase db) throws IOException {
		SQLDatabase dbFinal = Objects.requireNonNull(db, "db");
		RunOnce.ensure(s_fInit, () -> {
			reqQ = new RepQueries(dbFinal.getName(), dbFinal.getType());
		});
	}

	public static class RepQueries {

		SQLTableSchema tsRepo, tsCommit;

		SQLQuery qRepoIns, qRepoSelId, qRepoSelSchema, qRepoUpdCommit, qRepoUpdChange, qRepoDelOne;

		SQLQuery qCommitIns,  qCommitSelRepo, qCommitDelOne,qCommitSeq;

		RepQueries(String dbName, DBType dbType) throws IOException {

			// (name,fieldType(canonicalName),[flags(0,1),nSpit])

			tsRepo = new SQLTableSchema(SREPO_TABLE);
			tsRepo.parseAndAdd(new String[] { "id,string(48),1", "tschema,string(16),1", "tenant_id,int,1",
					"ao_id,int,1","commit_dir,string,0","ext_keep,string,1","commit_id,int,1", "commit_date,date,1", "change_date,date,1", "create_date,date,1",
					"last_update,date,1" });

			String stPrimaryKey = "id";
			String[] aUnique = new String[] { "tschema,tenant_id,ao_id" };
			String[] aIndex = null;
			tsRepo.setCreateScriptParams(stPrimaryKey, aUnique, aIndex, Tenant.FILE_GROUP, null);
			tsRepo.setReadOnly();
			SQLTableSchema.register(tsRepo);

			// This is only for logging (Developers can use this to create scripts suitable
			// to any
			// supported database.
			MSSQLUtil.getCreateTableScript(tsRepo);

			qRepoIns = DriverUtil.createInsertQuery(dbName, dbType, tsRepo);
			qRepoSelId = DriverUtil.createSelectQuery(dbName, dbType, tsRepo, "id", "ById");
			qRepoSelSchema = DriverUtil.createSelectQuery(dbName, dbType, tsRepo, "tschema", "BySchema");
			qRepoUpdCommit = DriverUtil.createUpdateQuery(dbName, dbType, tsRepo, "commit_id,commit_date,last_update",
					"id", "Commit");
			qRepoUpdChange = DriverUtil.createUpdateQuery(dbName, dbType, tsRepo, "change_date,last_update", "id",
					"Change");
			qRepoDelOne = DriverUtil.createDeleteQuery(dbName, dbType, tsRepo, "id", "One");
			SQLQuery.addToMap(qRepoIns, qRepoSelId, qRepoSelSchema, qRepoUpdCommit, qRepoUpdChange, qRepoDelOne);

			cacheRepo = BackgroundTask.get().createCache(SREPO_TABLE, true);

			tsCommit = new SQLTableSchema(SCOMMIT_TABLE);
			tsCommit.parseAndAdd(new String[] { "id,int,1", "repo_id,string(48),1", "chash,string(128),1",
					"tenant_id,int,1", "create_date,date,1", "info,string,0,3" });

			stPrimaryKey = "id";
			aUnique = new String[] { "repo_id,chash" };
			aIndex = null;
			
			tsCommit.setCreateScriptParams(stPrimaryKey, aUnique, aIndex, Tenant.FILE_GROUP, SCOMMIT_SEQ);
			tsCommit.setReadOnly();
			SQLTableSchema.register(tsCommit);

			MSSQLUtil.getCreateTableScript(tsCommit);
			MSSQLUtil.getCreateSequenceScript(SCOMMIT_SEQ);

			qCommitSeq = DriverUtil.createSequenceQuery(dbName, dbType, tsCommit, SCOMMIT_SEQ);
			qCommitIns = DriverUtil.createInsertQuery(dbName, dbType, tsCommit);
			
			qCommitSelRepo = DriverUtil.createSelectQuery(dbName, dbType, tsCommit, "repo_id", "ByRepo");
			qCommitDelOne = DriverUtil.createDeleteQuery(dbName, dbType, tsCommit, "id", "One");
			SQLQuery.addToMap(qCommitIns, qCommitSelRepo, qCommitDelOne,qCommitSeq);
		}
	}
	

	
	static int generateCommitId() throws IOException {
		return (int) SQLTm.get().nextSequenceId(reqQ.qCommitSeq);
	}
	
	public static class SCommit {
		int id;
	
		String chash,repoId;
		int tenantId;
		Date dateCreate;
		Map<String,String> mapInfo;

		public SCommit() {
		}

		public SCommit(SRepo repo, String chash,Map<String, String> mapInfo,int commitId) throws IOException {
			//Generated from sequence.
			if (commitId <=0 || commitId <= repo.getCommitId()) {
				throw new SolderException("Invalid commitId "+commitId);
			}
			this.id = commitId;
			Objects.requireNonNull(repo,"repo");
			this.repoId = repo.getId();
			
			this.chash = Validator.require(chash, "chash", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
			this.tenantId = repo.getTenantId();
		
			dateCreate = new Date();
			if (mapInfo == null) {
				mapInfo = new LinkedHashMap<>();
			}
			this.mapInfo = mapInfo;
		}

		public void serialize(Encoder encoder) throws IOException {
			encoder.writeInt("id", id);
			encoder.writeString("repo_id", repoId);
			encoder.writeString("chash", chash);
			encoder.writeInt("tenant_id", tenantId);
			encoder.writeDate("create_date", dateCreate);
			encoder.writeProperties("info", mapInfo);
		}

		public void deserialize(Decoder decoder) throws IOException {
			
			id = decoder.readInt("id");
			repoId = decoder.readString("repo_id");
			chash = decoder.readString("chash");
			tenantId = decoder.readInt("tenant_id");
			dateCreate = decoder.readDate("create_date");
			mapInfo = decoder.readProperties("info");
		}

		

		public int getId() {
			return id;
		}

		public String getRepoId() {
			return repoId;
		}

		public String getCHash() {
			return chash;
		}

		public int getTenantId() {
			return tenantId;
		}

		
		public Date getCreateDate() {
			return dateCreate;
		}

		public Map<String, String> getInfo() {
			return mapInfo;
		}

		

		void insert() throws IOException {
			// We get the transactions sqlTm.
			if (id < 0) {
				throw new SolderException("Commit Id not set");
			}
			SQLTm.get().executeOne(reqQ.qCommitIns, this::serialize, null);
			Event.log(SEvent.SCommitCreate,id , tenantId, (mb) -> {
				mb.put("repo_id", repoId);
				mb.put("chash", chash);
			});
		}
		
		public void delete() throws IOException {
			int i = SQLTm.get().executeOne(reqQ.qCommitDelOne, (encoder) -> {
				encoder.writeInt("id", id);
			}, null);

			if (i < 1) {
				Event.log(EEvent.DbDeleteFail, id, tenantId, (mb) -> {
					mb.put("table", "srepo");
					mb.put("id", id);
				});
			} else {
				Event.log(SEvent.SCommitDelete,id , tenantId, (mb) -> {
					mb.put("repo_id", repoId);
					mb.put("chash", chash);
				});
			}

		}
		
		
	}

	static List<SCommit> selectByRepo(String repoId) throws IOException {
		String repoIdFinal = Validator.require(repoId, "repo_id", Rules.NO_NULL_EMPTY,Rules.TRIM);
		List<SCommit> list = new ArrayList<>();
		SQLTm.get().select(reqQ.qCommitSelRepo, (encoder) -> {
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

	public static class SRepo {
		String id, schemaName,commitDir;
		String[] aExtension;
		int tenantId, aoId, commitId;
		Date dateCommit, dateChange, dateCreate, dateUpdate;

		public SRepo() {
		}

		public SRepo(String id, String schemaName, int tenantId, int aoId,String commitDir,String[] aExtensions) throws IOException {
			this.id = Validator.require(id, "id", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
			this.schemaName = Validator.require(schemaName, "schema name", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
			
			if (commitDir==null) {
				commitDir="";
			} else {
				commitDir=commitDir.trim();
			}
			this.commitDir = commitDir;
			
			if (aExtensions != null) {
				for (int i=0;i<aExtensions.length;i++) {
					aExtensions[i] = aExtensions[i].trim().toLowerCase();
				}
			}
			
			this.aExtension=aExtensions;
			
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
			insert();
		}
		
		

		public void serialize(Encoder encoder) throws IOException {
			encoder.writeString("id", id);
			encoder.writeString("tschema", schemaName);
			encoder.writeInt("tenant_id", tenantId);
			encoder.writeInt("ao_id", aoId);
			encoder.writeString("commit_dir", commitDir);
			encoder.writeStringArray("ext_keep", aExtension);
			encoder.writeInt("commit_id", commitId);
			encoder.writeDate("commit_date", dateCommit);
			encoder.writeDate("change_date", dateChange);
			encoder.writeDate("create_date", dateCreate);
			encoder.writeDate("last_update", dateUpdate);
		}

		public void deserialize(Decoder decoder) throws IOException {
			id = decoder.readString("id");
			schemaName = decoder.readString("tschema");
			tenantId = decoder.readInt("tenant_id");
			aoId = decoder.readInt("ao_id");
			commitDir = decoder.readString("commit_dir");
			aExtension = decoder.readStringArray("ext_keep");
			commitId = decoder.readInt("commit_id");
			dateCommit = decoder.readDate("commit_date");
			dateChange = decoder.readDate("change_date");
			dateCreate = decoder.readDate("create_date");
			dateUpdate = decoder.readDate("last_update");
		}

		public String[] cacheKeys() {
			return new String[] { CacheHelper.getKey(CacheHelper.KEY_ID, id),
					CacheHelper.getKey(CacheHelper.KEY_TENANT_TYPE_NAME, tenantId, schemaName, String.valueOf(aoId)) };
		}

		public String getId() {
			return id;
		}

		public String getName() {
			return getId();
		}

		public String getSchemaName() {
			return schemaName;
		}

		public int getTenantId() {
			return tenantId;
		}

		public int getAoId() {
			return aoId;
		}
		
		public String getCommitDir() {
			return commitDir;
		}
		
		public String[] getExtensionToKeep() {
			return aExtension;
		}

		public int getCommitId() {
			return commitId;
		}

		public Date getCommitDate() {
			return dateCommit;
		}

		public Date getChangeDate() {
			return dateChange;
		}

		public Date getCreateDate() {
			return dateCreate;
		}

		public Date getLastDate() {
			return dateUpdate;
		}

		// For now copy everything so we get testing..
		// We can optimize by not copying when the file is locally available..

		public IVaultProvider getProvider(boolean fReadOnly) throws IOException {
			// Pick a Cache Directory...
			SyncLocalRepo syncCache = SyncLocalRepo.get(SyncLocalRepo.DEFAULT);
			File fileProv = syncCache.ensureSyncFolder(id);
			return new FileVaultProvider(fileProv.getAbsolutePath(), fReadOnly);
		}

		void insert() throws IOException {
			// We get the transactions sqlTm.

			SQLTm.get().executeOne(reqQ.qRepoIns, this::serialize, null);

			// Add to the cache
			cacheRepo.store(this, this::cacheKeys);
			Audit.audit(SAudit.SRepo_Create, aoId, -1, tenantId, (cmb) -> {
				cmb.put("id", id);
				cmb.put("schema", schemaName);
			});
		}

		public void updateChange(Date dateChange) throws IOException {

			Date dateChangeFinal = Objects.requireNonNull(dateChange);
			Date dateUpdateNew = new Date();

			int i = SQLTm.get().executeOne(reqQ.qRepoUpdChange, (encoder) -> {

				encoder.writeDate("change_date", dateChange);
				encoder.writeDate("last_update", dateUpdateNew);

				// Where clause come
				encoder.writeString("id", id);

			}, null);
			if (i == 1) {
				this.dateChange = dateChangeFinal;
				this.dateUpdate = dateUpdateNew;

				Audit.audit(SAudit.SRepo_Update, aoId, -1, tenantId, (cmb) -> {
					cmb.put("id", id);
					cmb.put("op", "change_date");
					cmb.putIfChanged("change_date", dateChangeFinal, dateChange);
				});

			} else {
				Event.log(SEvent.DbUpdateFail, aoId, tenantId, (mb) -> {
					mb.put("table", "srepo");
					mb.put("id", id);
				});
			}

		}

		public void updateCommit(SCommit commit) throws IOException {

			Objects.requireNonNull(commit);
			int commitIdNew = commit.getId();
			Date dateCommitNew = commit.getCreateDate();
			if (commitIdNew <= this.commitId) {
				throw new SolderException("Commit id sequencing error; new commit " + commitIdNew
						+ " must be newer than current commit " + commitId);
			}

			Date dateUpdateNew = new Date();

			int i = SQLTm.get().executeOne(reqQ.qRepoUpdCommit, (encoder) -> {
				encoder.writeInt("commit_id", commitIdNew);
				encoder.writeDate("commit_date", dateCommitNew);
				encoder.writeDate("last_update", dateUpdateNew);

				// Where clause come
				encoder.writeString("id", id);
			}, null);
			if (i == 1) {
				Audit.audit(SAudit.SRepo_Update, aoId, -1, tenantId, (cmb) -> {
					cmb.put("id", id);
					cmb.put("op", "rep_info");
					cmb.putIfChanged("commit_id", commitIdNew, commitId);
					cmb.putIfChanged("commit_date", dateCommitNew, dateCommit);

				});
				this.commitId = commitIdNew;
				this.dateCommit = dateCommitNew;
				this.dateUpdate = dateUpdateNew;

			} else {
				Event.log(SEvent.DbUpdateFail, aoId, tenantId, (mb) -> {
					mb.put("table", "srepo");
					mb.put("id", id);
					mb.put("commit_id", commitIdNew);
				});
			}

		}

		public void delete() throws IOException {
			int i = SQLTm.get().executeOne(reqQ.qRepoDelOne, (encoder) -> {
				encoder.writeString("id", id);
			}, null);

			if (i < 1) {
				Event.log(EEvent.DbDeleteFail, aoId, tenantId, (mb) -> {
					mb.put("table", "srepo`");
					mb.put("id", id);
				});
			} else {
				Audit.audit(SAudit.SRepo_Delete, aoId, -1, tenantId, (cmb) -> {
					cmb.put("id", id);
					cmb.put("schema", schemaName);
				});
			}

		}

		public void repInit() throws IOException {
			Event.log(SEvent.SRepClone, aoId, tenantId, (mb) -> {
				mb.put("table", "srepo");
				mb.put("id", id);
			});

			SyncLocalRepo syncCache = SyncLocalRepo.get(SyncLocalRepo.DEFAULT);
			File fileLocalRepo = syncCache.ensureSyncFolder(id);
			try {
				SolderRepoOps.repInit(this, fileLocalRepo);
			} catch (Exception e) {
				Event.log(SEvent.SRepError, aoId, tenantId, (mb) -> {
					mb.put("table", "srepo`");
					mb.put("id", id);
					mb.put("error", PrintUtils.getStackTrace(e));
				});
				throw SolderException.rethrow(e);
			}
		}
	}

	public static SRepo getById(String id) throws IOException {
		String idFinal = Validator.require(id, "id", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);

		String key = CacheHelper.getKey(CacheHelper.KEY_ID, idFinal);
		return cacheRepo.getStoreIfAbsent(key, () -> selectById(idFinal), (srepo) -> srepo.cacheKeys());

	}

	static SRepo selectById(String id) throws IOException {
		TReference<SRepo> tref = new TReference<>();
		SQLTm.get().select(reqQ.qRepoSelId, (encoder) -> {
			encoder.writeString("id", id);
		}, (decoder) -> {
			if (decoder.next()) {
				SRepo srepo = new SRepo();
				srepo.deserialize(decoder);
				tref.set(srepo);
			}
		}, null);
		return tref.get();
	}

	public static List<SRepo> selectBySchema(String schemaName) throws IOException {
		String schemaNameFinal = Validator.require(schemaName, "schema name", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
		List<SRepo> list = new ArrayList<>();
		SQLTm.get().select(reqQ.qRepoSelSchema, (encoder) -> {
			encoder.writeString("tschema", schemaNameFinal);
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
