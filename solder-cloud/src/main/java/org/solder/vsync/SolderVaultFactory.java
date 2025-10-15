package org.solder.vsync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.core.SAudit;
import org.solder.core.SEvent;

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



public class SolderVaultFactory implements IVaultFactory{
	
	private static Log LOG = LogFactory.getLog(SolderVaultFactory.class.getName());
	
	public static final String TYPE = "solder_vf";
	
	public SolderVaultFactory() {
		TVault.registerFactory(this); 
	}
	
	public String getType() {
		return TYPE;
	}
	
	public IVaultProvider getProvider(String stFactoryParams,boolean fReadOnly) throws IOException {
		String id = stFactoryParams;
		 SVault svault = getById(id);
		 Objects.requireNonNull(svault,"SVault "+id);
		 return svault.getProvider(fReadOnly);
	}
	
	
	
	static final String SVAULT_TABLE = "svault";
	
	
	static AtomicBoolean s_fInit = new AtomicBoolean(false);
	static VaultQueries vaultQ = null;
	static Cache<SVault> cacheSVault = null;
	
	

	public static void init(SQLDatabase db) throws IOException {
		SQLDatabase dbFinal = Objects.requireNonNull(db, "db");
		RunOnce.ensure(s_fInit, () -> {
			vaultQ = new VaultQueries(dbFinal.getName(), dbFinal.getType());
		});
	}

	public static class VaultQueries {

		SQLTableSchema tsVault;

		SQLQuery qVaultIns, qVaultSelAll, qVaultUpdRep,qVaultUpdChange, qVaultDelOne;

		VaultQueries(String dbName, DBType dbType) throws IOException {

			// (name,fieldType(canonicalName),[flags(0,1),nSpit])

			tsVault = new SQLTableSchema(SVAULT_TABLE);
			tsVault.parseAndAdd(new String[] { "id,string(48),1", "tschema,string(16),1","tenant_id,int,1", "ao_id,int,1",
					"rep_info,string,0,3","rep_date,date,1","change_date,date,1", "create_date,date,1", "last_update,date,1" });
			
			

			String stPrimaryKey = "id";
			String[] aUnique = new String[] {"tschema,tenant_id,ao_id"};
			String[] aIndex = null;
			tsVault.setCreateScriptParams(stPrimaryKey, aUnique, aIndex, Tenant.FILE_GROUP, null);
			tsVault.setReadOnly();
			SQLTableSchema.register(tsVault);

			// This is only for logging (Developers can use this to create scripts suitable
			// to any
			// supported database.
			MSSQLUtil.getCreateTableScript(tsVault);

			qVaultIns = DriverUtil.createInsertQuery(dbName, dbType, tsVault);
			qVaultSelAll = DriverUtil.createSelectQuery(dbName, dbType, tsVault, null, "All");
			qVaultUpdRep = DriverUtil.createUpdateQuery(dbName, dbType, tsVault, "rep_info,rep_date,last_update", "id",
					"Rep");
			qVaultUpdChange = DriverUtil.createUpdateQuery(dbName, dbType, tsVault, "change_date,last_update", "id",
					"Change");
			qVaultDelOne = DriverUtil.createDeleteQuery(dbName, dbType, tsVault, "id", "One");
			SQLQuery.addToMap(qVaultIns, qVaultSelAll, qVaultUpdRep,qVaultUpdChange, qVaultDelOne);
			
			cacheSVault = BackgroundTask.get().createCache(SVAULT_TABLE, false);
		}
	}

	
	public static void syncObjects() throws IOException{
		List<SVault> list = selectAll();
		
		cacheSVault.reloadAll((_) -> {
			for (SVault svault : list) {
				cacheSVault.store(svault, svault::cacheKeys);
			}
		});
		
	}
	
	public enum VaultSchemaName {
		BMESSAGE_INDEX("bmsg_index");
		String canonicalName;
		private VaultSchemaName(String name) {
			canonicalName = Validator.require(name, "name", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
		}
		
		public String getCanonicalName() {
			return canonicalName;
		}
	}

	
	String id;
	String schemaName;
	int tenantId,aoId;
	
	
	public static class SVault  {
		String id,schemaName; 
		int tenantId,aoId;
		
		
		//Each file in stored in BlobFS 
		//long fsId;
		//String relPath,digest;
		
		//Commits must be synced up.
		Map<String,String> repInfo;
		Date dateRep,dateChange,dateCreate,dateUpdate;
		
		public SVault() {}
		
		public SVault(String id,String schemaName,int tenantId,int aoId) throws IOException {
			this.id = Validator.require(id,"id",Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			this.schemaName = Validator.require(schemaName,"schema name",Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			this.tenantId=tenantId;
			this.aoId=aoId;
			repInfo = new HashMap<>();
			dateCreate=new Date();
			dateUpdate = dateCreate;
			//Just put date rep some number less than create..
			dateRep = new Date(dateCreate.getTime()-TimeUnit.DAYS.toMillis(30));
			dateChange = dateCreate;
			//Create scache before you insert..
			getProvider(false);
			insert();
		}
		
		
		public void serialize(Encoder encoder) throws IOException {
			encoder.writeString("id", id);
			encoder.writeString("tschema", schemaName);
			encoder.writeInt("tenant_id", tenantId);
			encoder.writeInt("ao_id", aoId);
			encoder.writeProperties("rep_info", repInfo);
			encoder.writeDate("rep_date", dateRep);
			encoder.writeDate("change_date", dateChange);
			encoder.writeDate("create_date", dateCreate);
			encoder.writeDate("last_update", dateUpdate);
		}

		public void deserialize(Decoder decoder) throws IOException {
			id = decoder.readString("id");
			schemaName = decoder.readString("tschema");
			tenantId = decoder.readInt("tenant_id");
			aoId = decoder.readInt("ao_id");
			repInfo = decoder.readProperties("rep_info");
			
			dateRep = decoder.readDate("rep_date");
			dateChange = decoder.readDate("change_date");
			dateCreate = decoder.readDate("create_date");
			dateUpdate = decoder.readDate("last_update");
		}
		
		public String[] cacheKeys() {
			return new String[] { CacheHelper.getKey(CacheHelper.KEY_ID, id), CacheHelper.getKey(CacheHelper.KEY_TENANT_TYPE_NAME,tenantId,schemaName,String.valueOf(aoId)) };
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

		public Map<String, String> getRepInfo() {
			return repInfo;
		}

		public Date getRepDate() {
			return dateRep;
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
		
		
		//For now copy everything so we get testing..
		//We can optimize by not copying when the file is locally available..
		
		public IVaultProvider getProvider(boolean fReadOnly) throws IOException {
				//Pick a Cache Directory...
				SyncCache syncCache = SyncCache.get(SyncCache.DEFAULT);
				File fileProv = syncCache.ensureSyncFolder(id);
				return new FileVaultProvider(fileProv.getAbsolutePath(),fReadOnly); 
		}
	
		
		void insert() throws IOException {
			// We get the transactions sqlTm.
			
			SQLTm.get().executeOne(vaultQ.qVaultIns, this::serialize, null);

			// Add to the cache
			cacheSVault.store(this, this::cacheKeys);
			Audit.audit(SAudit.SVault_Create,aoId,-1,tenantId, (cmb)->{
				cmb.put("id", id);
				cmb.put("schema", schemaName);
			});
		}
		
		public void updateChange(Date dateChange) throws IOException {
			
			
			
			Date dateChangeFinal = Objects.requireNonNull(dateChange);
			Date dateUpdateNew = new Date();

			int i=SQLTm.get().executeOne(vaultQ.qVaultUpdChange,(encoder)->{
				
				encoder.writeDate("change_date", dateChange);
				encoder.writeDate("last_update", dateUpdateNew);
				
				//Where clause come
				encoder.writeString("id", id);
				
			},null);
			if (i == 1) {
				this.dateChange=dateChangeFinal;
				this.dateUpdate=dateUpdateNew;
				
				Audit.audit(SAudit.SVault_Update, aoId, -1, tenantId, (cmb) -> {
					cmb.put("id", id);
					cmb.put("op", "change_date");
					cmb.putIfChanged("change_date", dateChangeFinal,dateChange);
				});
				
			} else {
				Event.log(SEvent.DbUpdateFail, -1, -1, (mb) -> {
					mb.put("table", "svault");
					mb.put("id", id);
				});
			}
			
		}
		
		public void updateRep(Map<String,String> repInfoNew,Date dateRepNew) throws IOException {
			
			Date dateRepNewFinal = Objects.requireNonNull(dateRepNew);
			Map<String, String> repInfo2 = repInfo;
			
			if (repInfoNew != null) {
				repInfo2 = new LinkedHashMap<>();
				repInfo2.putAll(repInfoNew);
			}
			Map<String, String> repInfoNewFinal = repInfo2;
			
			
			Date dateUpdateNew = new Date();

			int i=SQLTm.get().executeOne(vaultQ.qVaultUpdRep,(encoder)->{
				encoder.writeProperties("rep_info", repInfoNewFinal);
				encoder.writeDate("rep_date", dateRepNewFinal);
				encoder.writeDate("last_update", dateUpdateNew);
				
				//Where clause come
				encoder.writeString("id", id);
				
			},null);
			if (i == 1) {
				Audit.audit(SAudit.SVault_Update, aoId, -1, tenantId, (cmb) -> {
					cmb.put("id", id);
					cmb.put("op", "rep_info");
					cmb.putIfChanged("rep_date", dateRepNewFinal,dateRep);
					cmb.putIfChanged("props", repInfoNewFinal, repInfo);
				});
				
				this.repInfo=repInfoNew;
				this.dateRep=dateRepNewFinal;
				this.dateUpdate=dateUpdateNew;
				
			} else {
				Event.log(SEvent.DbUpdateFail, -1, -1, (mb) -> {
					mb.put("table", "svault");
					mb.put("id", id);
				});
			}
			
		}
		
		
		public void delete() throws IOException {
			int i = SQLTm.get().executeOne(vaultQ.qVaultDelOne, (encoder) -> {
				encoder.writeString("id", id);
			}, null);

			if (i < 1) {
				Event.log(EEvent.DbDeleteFail, -1, -1, (mb) -> {
					mb.put("table", "svault");
					mb.put("id", id);
				});
			} else {
				Audit.audit(SAudit.SVault_Delete, aoId, -1, tenantId, (cmb) -> {
					cmb.put("id", id);
					cmb.put("schema", schemaName);
				});
			}

		}
	}
	
	public static SVault getById(String id) {
		id = Validator.require(id, "id", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
		// We cache all Objects refresh it periodically.
		return cacheSVault.get(CacheHelper.getKey(CacheHelper.KEY_ID, id));
	}
	
	public static List<SVault> getAll() {
		return cacheSVault.getAll();
	}
	
	static List<SVault> selectAll() throws IOException {
		List<SVault> list = new ArrayList<>();
		SQLTm.get().select(vaultQ.qVaultSelAll, null, (decoder) -> {
			while (decoder.next()) {
				SVault svault = new SVault();
				svault.deserialize(decoder);
				list.add(svault);
			}
		}, null);
		return list;
	}

}

