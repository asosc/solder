package org.solder.dbSnap;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.function.IOPredicate;
import org.solder.core.SEvent;

import com.beech.store.TField;
import com.beech.store.TSchema;
import com.beech.store.TSchema.SchemaType;
import com.ee.session.SQLTm;
import com.ee.session.db.EEvent;
import com.ee.session.db.Event;
import com.ee.session.db.Tenant;
import com.ee.util.exception.EException;
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

public class TSnap implements ISerializable {

	public static final String TSNAP_TABLE = "tsnap";
	public static final String TSNAP_SEQ = "tsnap_seq";
	
	public static final String FN_NAME = "name";
	public static final String FN_SUBSET = "subset";
	public static final String FN_CRITERIA = "criteria";
	public static final String FN_NEXT_CRITERIA = "next_criteria";
	public static final String FN_CREATE_DATE = "create_date";
	public static final String FN_PROPS = "props";
	public static final String FN_ID = "id";
	public static final String FN_NIMPORT = "n_import";
	public static final String FN_NIGNORED = "n_ignored";
	public static final String FN_NTOTAL = "n_total";

	public static TSchema getSchema(String schemaVersion) throws IOException {

		schemaVersion = Validator.require(schemaVersion, "schema version", Rules.TRIM, Rules.NO_NULL_EMPTY);
		Set<TField> setField = Set.of(new TField(FN_NAME, FieldType.STRING, null, TField.FLAG_NOT_NULL),
				new TField(FN_SUBSET, FieldType.STRING, null, TField.FLAG_NONE),
				new TField(FN_CRITERIA, FieldType.STRING, null, TField.FLAG_NONE),
				new TField(FN_NEXT_CRITERIA, FieldType.STRING, null, TField.FLAG_NONE),
				new TField(FN_CREATE_DATE, FieldType.DATE, null, TField.FLAG_NOT_NULL),
				new TField(FN_PROPS, FieldType.PROP, null, TField.FLAG_NONE),
				new TField(FN_ID, FieldType.INT, null, TField.FLAG_NOT_NULL),
				new TField(FN_NIMPORT, FieldType.INT, null, TField.FLAG_NONE),
				new TField(FN_NIGNORED, FieldType.INT, null, TField.FLAG_NONE),
				new TField(FN_NTOTAL, FieldType.INT, null, TField.FLAG_NONE));

		return new TSchema(TSNAP_TABLE, schemaVersion, SchemaType.REGULAR, setField, null);
	}
	
	static AtomicBoolean s_fInit = new AtomicBoolean(false);

	static TSnapQueries tsnapQ = null;

	public static void init(SQLDatabase db) throws IOException {
		SQLDatabase dbFinal = Objects.requireNonNull(db, "db");
		RunOnce.ensure(s_fInit, () -> {
			tsnapQ = new TSnapQueries(dbFinal.getName(), dbFinal.getType());
		});
	}

	public static DBType getDBType() {
		return tsnapQ.dbType;
	}

	

	public static class TSnapQueries {

		SQLTableSchema tsTsnap;
		DBType dbType;

		SQLQuery qTsnapIns, qTsnapSelId,qTsnapSelName,qTsnapSelLatest,qTsnapUpd, qTsnapDelOne,qTsnapSeq;

		TSnapQueries(String dbName, DBType dbType) throws IOException {

			Objects.requireNonNull(dbType, "dbType");
			this.dbType = dbType;

			tsTsnap = new SQLTableSchema(TSNAP_TABLE);
			tsTsnap.parseAndAdd(new String[] { "id,int,1","name,string(64),1", "subset,string,1", 
					"criteria,string,1","next_criteria,string,1", 
					"create_date,date,1", "props,string,0,3",
					"n_import,int,1","n_ignored,int,1","n_total,int,1"});

			String stPrimaryKey = "id";
			String[] aUnique = null;
			String[] aIndex = new String[] { "name,subset,create_date"};
			tsTsnap.setCreateScriptParams(stPrimaryKey, aUnique, aIndex, Tenant.FILE_GROUP, TSNAP_SEQ);
			tsTsnap.setSerializerFieldType("props", FieldType.PROP);
			tsTsnap.setReadOnly();
			SQLTableSchema.register(tsTsnap);

			// This is only for logging (Developers can use this to create scripts suitable
			// to any
			// supported database.
			MSSQLUtil.getCreateTableScript(tsTsnap);
			String stSeqCreate = MSSQLUtil.getCreateSequenceScript(TSNAP_SEQ);

			
			
			qTsnapSeq = DriverUtil.createSequenceQuery(dbName, dbType, tsTsnap, TSNAP_SEQ);

			qTsnapIns = DriverUtil.createInsertQuery(dbName, dbType, tsTsnap);
			qTsnapSelId  = DriverUtil.createSelectQuery(dbName, dbType, tsTsnap, "id", "id");
			qTsnapSelName = DriverUtil.createSelectQuery(dbName, dbType, tsTsnap, "name,subset", "Name");
			
			qTsnapSelLatest = DriverUtil.createSelectQuery(dbName, dbType, tsTsnap, "", "Latest", (sb) -> {
				sb.append(" ORDER BY create_date DESC");
			}, null);

			qTsnapUpd = DriverUtil.createUpdateQuery(dbName, dbType, tsTsnap, "next_criteria,props,n_import,n_ignored,n_total", "id",
					"upd");
			
			qTsnapDelOne = DriverUtil.createDeleteQuery(dbName, dbType, tsTsnap, "id", "One");
			SQLQuery.addToMap(qTsnapIns, qTsnapSelId, qTsnapSelName, qTsnapSelLatest, qTsnapDelOne, qTsnapSeq);

		}
	}
	
	static int generateId() throws IOException {
		return (int) SQLTm.get().nextSequenceId(tsnapQ.qTsnapSeq);
	}

	
	String name,subset,criteria,nextCriteria;
	Date dateCreate;
	Map<String,String> props;
	int id,nImport,nIgnored,nTotal;

	public TSnap() {

	}

	public TSnap(String name,String subset,String criteria,String nextCriteria,Map<String,String> props,int nTotal,int nImport,int nIgnored) throws IOException{
		this.name = Validator.require(name, "name", Rules.TRIM_LOWER,Rules.NO_NULL_EMPTY);
		this.subset =  Validator.require(subset, "subset", Rules.TRIM_LOWER,Rules.NO_NULL_EMPTY);
		this.criteria = criteria;
		this.nextCriteria = nextCriteria;
		this.dateCreate = new Date();
		this.props = props;
		this.nTotal = nTotal;
		this.nImport = nImport;
		this.nIgnored = nIgnored;
		
		this.id = generateId();
		insert();
	}
	

	public void serialize(Encoder encoder) throws IOException {
		encoder.writeInt(FN_ID, id);
		encoder.writeString(FN_NAME, name);
		encoder.writeString(FN_SUBSET, subset);
		encoder.writeString(FN_CRITERIA, criteria);
		encoder.writeString(FN_NEXT_CRITERIA, nextCriteria);
		encoder.writeDate(FN_CREATE_DATE, dateCreate);
		encoder.writeProperties(FN_PROPS, props);
		encoder.writeInt(FN_NIMPORT, nImport);
		encoder.writeInt(FN_NIGNORED, nIgnored);
		encoder.writeInt(FN_NTOTAL, nTotal);
	}

	public void deserialize(Decoder decoder) throws IOException {
		id = decoder.readInt(FN_ID);
		name = decoder.readString(FN_NAME);
		subset = decoder.readString(FN_SUBSET);
		criteria = decoder.readString(FN_CRITERIA);
		nextCriteria = decoder.readString(FN_NEXT_CRITERIA);
		dateCreate = decoder.readDate(FN_CREATE_DATE);
		props = decoder.readProperties(FN_PROPS);
		nImport = decoder.readInt(FN_NIMPORT);
		nIgnored = decoder.readInt(FN_NIGNORED);
		nTotal = decoder.readInt(FN_NTOTAL);
	}

	
	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getSubset() {
		return subset;
	}

	public String getCriteria() {
		return criteria;
	}

	public String getNextCriteria() {
		return nextCriteria;
	}

	public Date getCreateDate() {
		return dateCreate;
	}

	public Map<String, String> getProps() {
		return props;
	}

	public int getImportCount() {
		return nImport;
	}

	public int getIgnoredCount() {
		return nIgnored;
	}

	public int getTotalCount() {
		return nTotal;
	}
	
	public String toString() {
		return String.format("TSnap %d[%s %s] crit=%s nextCrit=%s (nImp=%d,nIg=%d,nTot=%d)",id,name,subset,criteria,nextCriteria,nImport,nIgnored,nTotal);
	}

	
	void insert() throws IOException {
		// We get the transactions sqlTm.
		if (id < 0) {
			// Generate ID
			throw new EException(
					"Valid TSnap id not set (Use generateId); id=" + id);
		}
		SQLTm.get().executeOne(tsnapQ.qTsnapIns, this::serialize, null);
		
		Event.log(SEvent.TsnapSync, id, -1, (mb) -> {
			mb.put(FN_NAME, name);
			mb.put(FN_SUBSET, subset);
			mb.put(FN_CRITERIA, criteria);
			mb.put(FN_NEXT_CRITERIA, nextCriteria);
		});
	}
	
	
	public void update(String nextCriteriaNew,Map<String,String> propsNew,int nImportNew,int nIgnoredNew,int nTotalNew) throws IOException{
		
		String nextCriteriaFinal = Validator.updateValue(nextCriteriaNew, nextCriteria,null);
		
		int nImportFinal=nImportNew>=0?nImportNew:nImport;
		int nIgnoredFinal=nIgnoredNew>=0?nIgnoredNew:nIgnored;
		int nTotalFinal=nTotalNew>=0?nTotalNew:nTotal;

		Map<String, String> propsTemp = props;
		if (propsNew != null) {
			propsTemp = new LinkedHashMap<>();
			propsTemp.putAll(propsNew);
		}
		Map<String, String> propsFinal = propsTemp;

		int i = SQLTm.get().executeOne(tsnapQ.qTsnapUpd, (encoder) -> {

			encoder.writeString(FN_NEXT_CRITERIA, nextCriteria);
			encoder.writeProperties(FN_PROPS, props);
			encoder.writeInt(FN_NIMPORT, nImport);
			encoder.writeInt(FN_NIGNORED, nIgnored);
			encoder.writeInt(FN_NTOTAL, nTotal);

			// Where clause come
			encoder.writeInt(FN_ID, id);

		}, null);
		if (i == 1) {
			nextCriteria = nextCriteriaFinal;
			props = propsFinal;
			nImport = nImportFinal;
			nIgnored = nIgnoredFinal;
			nTotal = nTotalFinal;
			
		}else {
			Event.log(EEvent.DbUpdateFail,id,-1, (mb)->{
				mb.put("table", TSNAP_TABLE);
				mb.put(FN_NAME, name);
				mb.put(FN_SUBSET, subset);
				mb.put(FN_NEXT_CRITERIA, nextCriteriaFinal);
				mb.put(FN_PROPS, propsFinal);
				mb.put(FN_NIMPORT, nImportFinal);
				mb.put(FN_NIGNORED, nIgnoredFinal);
				mb.put(FN_NTOTAL, nTotalFinal);
			});
		} 
	}

	public void delete() throws IOException {
		int i = SQLTm.get().executeOne(tsnapQ.qTsnapDelOne, (encoder) -> {
			encoder.writeInt(FN_ID, id);
		}, null);
		
		if (i<1 ) {
			Event.log(EEvent.DbDeleteFail,id,-1, (mb)->{
				mb.put("table", TSNAP_TABLE);
				mb.put(FN_NAME, name);
				mb.put(FN_SUBSET, subset);
			});
		}

	}
	
	
	public static TSnap selectById(int id) throws IOException {

		TReference<TSnap> tref = new TReference<>();
		SQLTm.get().select(tsnapQ.qTsnapSelId, (encoder) -> {
			encoder.writeInt(FN_ID, id);
		}, (decoder) -> {
			if (decoder.next()) {
				TSnap tsnap = new TSnap();
				tsnap.deserialize(decoder);
				tref.set(tsnap);
			}
		}, null);
		return tref.get();
	}
	
	
	public static TSnap selectLatest(String name,String subset) throws IOException {
		String nameFinal = Validator.require(name, "name", Rules.TRIM_LOWER,Rules.NO_NULL_EMPTY);
		String subsetFinal =  Validator.require(subset, "subset", Rules.TRIM_LOWER,Rules.NO_NULL_EMPTY);
		TReference<TSnap> tref = new TReference<>();
		SQLTm.get().select(tsnapQ.qTsnapSelName, (encoder) -> {
			encoder.writeString(FN_NAME, nameFinal);
			encoder.writeString(FN_SUBSET, subsetFinal);
		}, (decoder) -> {
			//We only take the first record (there can be many
			if (decoder.next()) {
				TSnap tsnap = new TSnap();
				tsnap.deserialize(decoder);
				tref.set(tsnap);
			}
		}, null);
		return tref.get();
	}
	
	public static void selectByName(String name,String subset,IOPredicate<TSnap> cTsnap) throws IOException {
		String nameFinal = Validator.require(name, "name", Rules.TRIM_LOWER,Rules.NO_NULL_EMPTY);
		String subsetFinal =  Validator.require(subset, "subset", Rules.TRIM_LOWER,Rules.NO_NULL_EMPTY);
		Objects.requireNonNull(cTsnap);
		SQLTm.get().select(tsnapQ.qTsnapSelName, (encoder) -> {
			encoder.writeString(FN_NAME, nameFinal);
			encoder.writeString(FN_SUBSET, subsetFinal);
		}, (decoder) -> {
			//We only take the first record (there can be many
			boolean f=true;
			while (f && decoder.next()) {
				TSnap tsnap = new TSnap();
				tsnap.deserialize(decoder);
				f = cTsnap.test(tsnap);
			}
		}, null);
	}
	
	

}
