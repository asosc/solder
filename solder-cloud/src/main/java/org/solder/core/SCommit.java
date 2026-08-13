package org.solder.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nimbo.blobs.BlobFS;
import org.solder.rest.solder.SCommitInfo;
import org.solder.rest.solder.SRepoInfo;

import com.ee.session.SQLTm;
import com.ee.session.db.EEvent;
import com.ee.session.db.Event;
import com.ee.session.db.Tenant;
import com.jnk.util.TReference;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.lnk.jdbc.DBType;
import com.lnk.jdbc.DriverUtil;
import com.lnk.jdbc.MSSQLUtil;
import com.lnk.jdbc.SQLDatabase;
import com.lnk.jdbc.SQLQuery;
import com.lnk.jdbc.SQLTableSchema;
import com.lnk.lucene.RunOnce;
import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;
import com.lnk.serializer.FieldType;
import com.lnk.serializer.ISerializable;

public class SCommit implements ISerializable,Comparable<SCommit> {
	
	private static Log LOG = LogFactory.getLog(SCommit.class.getName());
	
	public static SCommitInfo makeSCommitInfo(SCommit scommit) {
		if (scommit==null) {
			return null;
		} else {
			SCommitInfo commitInfo = new SCommitInfo(scommit.id,scommit.repoSid,scommit.chash,
					scommit.idPrev,scommit.chashPrev,scommit.tenantId,scommit.blobFsId,scommit.dateCreate,scommit.mapInfo);
			commitInfo.setParent(scommit);
			return commitInfo;
		}
	}
	
	
	public static SCommit getSCommit(SCommitInfo scommitInfo) throws IOException {
		//No null support, apps can do that this is most common situation and we want early error.
		Objects.requireNonNull(scommitInfo,"scommitInfo");
		SCommit scommit = (SCommit)scommitInfo.getParent();
		if (scommit == null) {
			//probably throw...(only efficiency issue...
			scommit = SCommit.selectCommitById(scommitInfo.getId());
			//Need to check repo..
			Objects.requireNonNull(scommit,"srepo reload");
		}
		return scommit;
	}
	
	static final String SCOMMIT_TABLE = "scommit";
	static final String SCOMMIT_SEQ = "scommit_seq";

	static AtomicBoolean s_fInit = new AtomicBoolean(false);
	static CommitQueries commQ = null;
	

	public static void init(SQLDatabase db) throws IOException {
		SQLDatabase dbFinal = Objects.requireNonNull(db, "db");
		RunOnce.ensure(s_fInit, () -> {
			commQ = new CommitQueries(dbFinal.getName(), dbFinal.getType());
		});
	}
	
	
	public static class CommitQueries {

		SQLTableSchema tsCommit;

		SQLQuery qCommitIns, qCommitSelId, qCommitSelRepo, qCommitDelOne, qCommitSeq;

		CommitQueries(String dbName, DBType dbType) throws IOException {

			// (name,fieldType(canonicalName),[flags(0,1),nSpit])

		
			tsCommit = new SQLTableSchema(SCOMMIT_TABLE);
			tsCommit.parseAndAdd(new String[] { "id,int,1", "repo_sid,int,1", "chash,string(128),1",
					"prev_id,int,1","prev_chash,string(128),1",
					"tenant_id,int,1", "blob_fsid,long,1", "create_date,date,1", "info,string,0,3" });

			String stPrimaryKey = "id";
			String[] aUnique = new String[] { "repo_sid,prev_id,chash" };
			String[] aIndex = new String[] {};

			tsCommit.setCreateScriptParams(stPrimaryKey, aUnique, aIndex, Tenant.FILE_GROUP, SCOMMIT_SEQ);
			tsCommit.setSerializerFieldType("info", FieldType.PROP);
			tsCommit.setReadOnly();
			SQLTableSchema.register(tsCommit);

			MSSQLUtil.getCreateTableScript(tsCommit);
			MSSQLUtil.getCreateSequenceScript(SCOMMIT_SEQ);

			qCommitSeq = DriverUtil.createSequenceQuery(dbName, dbType, tsCommit, SCOMMIT_SEQ);
			qCommitIns = DriverUtil.createInsertQuery(dbName, dbType, tsCommit);
			qCommitSelId = DriverUtil.createSelectQuery(dbName, dbType, tsCommit, "id", "ById");
			qCommitSelRepo = DriverUtil.createSelectQuery(dbName, dbType, tsCommit, "repo_sid", "ByRepo");
			qCommitDelOne = DriverUtil.createDeleteQuery(dbName, dbType, tsCommit, "id", "One");
			SQLQuery.addToMap(qCommitIns, qCommitSelRepo, qCommitDelOne, qCommitSeq);
		}
	}
	
	
	public static int generateCommitId() throws IOException {
		return (int) SQLTm.get().nextSequenceId(commQ.qCommitSeq);
	}

	
	int id,idPrev,repoSid;

	 String chash, chashPrev;
	 long blobFsId;
	 int tenantId;
	 Date dateCreate;
	 Map<String, String> mapInfo;

		public SCommit() {
		}

		public SCommit(SRepo repo, String chash, Map<String, String> mapInfo, int commitId) throws IOException {
			// Generated from sequence.
			if (commitId <= 0 || commitId <= repo.getCommitId()) {
				throw new SolderException("Invalid commitId " + commitId);
			}
			this.id = commitId;
			Objects.requireNonNull(repo, "repo");
			this.repoSid = repo.getSeqId();
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
		
		

		
		public int compareTo(SCommit sc) {
			// Natural order is based on id.
			return Integer.compare(id, sc.id);
		}

		public boolean equals(Object o) {
			if (o instanceof SCommit sc) {
				return id == sc.id;
			} else {
				return false;
			}
		}

		public int hashCode() {
			return Integer.hashCode(id);
		}
		
		

		public void serialize(Encoder encoder) throws IOException {
			encoder.writeInt("id", id);
			encoder.writeInt("repo_sid", repoSid);
			encoder.writeString("chash", chash);
			encoder.writeInt("prev_id", idPrev);
			encoder.writeString("prev_chash", chashPrev);
			encoder.writeInt("tenant_id", tenantId);
			encoder.writeLong("blob_fsid", blobFsId);
			encoder.writeDate("create_date", dateCreate);
			encoder.writeProperties("info", mapInfo);
		}

		public void deserialize(Decoder decoder) throws IOException {

			id = decoder.readInt("id");
			repoSid = decoder.readInt("repo_sid");
			//JsonDecoder by default should skip over repo_id for 
			//client, otherwise we have init the repo again.
			
			chash = decoder.readString("chash");
			idPrev = decoder.readInt("prev_id");
			chashPrev = decoder.readString("prev_chash");
			tenantId = decoder.readInt("tenant_id");
			blobFsId=decoder.readLong("blob_fsid");
			dateCreate = decoder.readDate("create_date");
			mapInfo = decoder.readProperties("info");
		}

		public int getId() {
			return id;
		}
		
		public int getRepoSeqId() {
			return repoSid;
		}

		public String getCHash() {
			return chash;
		}
		
		public int getPrevId() {
			return idPrev;
		}
		
		public String getPrevCHash() {
			return chashPrev;
		}

		public int getTenantId() {
			return tenantId;
		}
		
		public long getBlobFsId() {
			return blobFsId;
		}
		
		public void setBlobFsId(long blobFsId) {
			this.blobFsId=blobFsId;
		}
		
		

		public Date getCreateDate() {
			return dateCreate;
		}

		public Map<String, String> getInfo() {
			return mapInfo;
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
			SQLTm.get().executeOne(commQ.qCommitIns, this::serialize, null);
			Event.log(SEvent.SCommitCreate, id, tenantId, (mb) -> {
				mb.put("repo_sid", repoSid);
				mb.put("chash", chash);
			});
		}

		public void delete() throws IOException {
			int i = SQLTm.get().executeOne(commQ.qCommitDelOne, (encoder) -> {
				encoder.writeInt("id", id);
			}, null);

			if (i < 1) {
				Event.log(EEvent.DbDeleteFail, id, tenantId, (mb) -> {
					mb.put("table", "srepo");
					mb.put("id", id);
					mb.put("repo_sid", repoSid);
				});
			} else {
				Event.log(SEvent.SCommitDelete, id, tenantId, (mb) -> {
					mb.put("repo_sid", repoSid);
					mb.put("chash", chash);
				});
			}

		}
	

	public static SCommit selectCommitById(int id) throws IOException {
		TReference<SCommit> tref = new TReference<>();
		SQLTm.get().select(commQ.qCommitSelId, (encoder) -> {
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

	static List<SCommit> selectCommitByRepo(int repoSeqId) throws IOException {
		if (repoSeqId<=0 ) {
			throw new SolderException("Invalid repoSeqId "+repoSeqId);
		}
		List<SCommit> list = new ArrayList<>();
		SQLTm.get().select(commQ.qCommitSelRepo, (encoder) -> {
			encoder.writeInt("repo_sid", repoSeqId);
		}, (decoder) -> {
			while (decoder.next()) {
				SCommit scommit = new SCommit();
				scommit.deserialize(decoder);
				list.add(scommit);
			}
		}, null);
		return list;
	}

}
