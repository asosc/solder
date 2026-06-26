package org.solder.dbSnap;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.function.IOBiConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.core.SolderException;

import com.aura.crypto.CryptoScheme;
import com.beech.bfs.Mode;
import com.beech.lucene.LuceneFieldSpec;
import com.beech.lucene.LuceneFieldType;
import com.beech.lucene.LuceneSchema;
import com.beech.lucene.StandardBAnalyzer;
import com.beech.store.FileVaultProvider;
import com.beech.store.LDirSuppliers;
import com.beech.store.TField;
import com.beech.store.TRecordEncoder;
import com.beech.store.TSchema;
import com.beech.store.TSchema.SchemaType;
import com.beech.store.TSegmentBuilder;
import com.beech.store.TVault;
import com.ee.session.SQLTm;
import com.jnk.util.PrintUtils;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.lnk.jdbc.DriverUtil;
import com.lnk.jdbc.SQLDatabase;
import com.lnk.jdbc.SQLField;
import com.lnk.jdbc.SQLQuery;
import com.lnk.jdbc.SQLTableSchema;
import com.lnk.lucene.LBytesRefBuilder;
import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;
import com.lnk.serializer.FieldType;

public class SQLDBSnap {
	
	private static Log LOG = LogFactory.getLog(SQLDBSnap.class.getName());

	
	//SQLDatabase should have all the tables registered.
	
	static String makeVersion(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("GMT"));
		
		cal.setTime(date);
		
		int iYear = cal.get(Calendar.YEAR)-2000;
		int iMonth = cal.get(Calendar.MONTH);
		int iDate = cal.get(Calendar.DATE);
		int iHour = cal.get(Calendar.HOUR_OF_DAY);
		return String.format("%03d%02d%02d%02d", iYear,iMonth,iDate,iHour);
	}
	
	//No Index..., StringIds(for compression), Table vs TSM etc.
	public static TSchema makeTSchema(SQLTableSchema schema,String versionPrefix,IOBiConsumer<String,String> cVersionHash) throws IOException {
		
		CryptoScheme cs = CryptoScheme.getDefault();
		
		LBytesRefBuilder brbHash = new LBytesRefBuilder();
		
		String tableName = schema.getName().toLowerCase();
		
		
		Map<String,TField> mapField = new LinkedHashMap<>();
		for (SQLField sqlField : schema.getSerializerFieldList()) {
			String fn = sqlField.getName().toLowerCase();
			if (sqlField.getParent()!=null) {
				throw new SolderException("Unexpected split field "+sqlField.getName());
			}
			
			FieldType ft = sqlField.getSerializerFieldType();
			//Change ft from String to StrindId
			//Only NOT NULL , but we want to capture everything in database
			TField field = new TField(fn,ft);
			mapField.put(fn,field);
			brbHash.appendChars(String.format("%s %s\r\n",fn,ft.canonicalName()));
		}
		
		String ver = makeVersion(new Date());
		if (versionPrefix != null) {
			versionPrefix = versionPrefix.trim();
		}
		if (StringUtils.isEmpty(versionPrefix)) {
			versionPrefix = "1.0.";
		} else {
			if (!versionPrefix.endsWith(".")) {
				versionPrefix = versionPrefix+".";
			}
		}
		
		String version = versionPrefix+ver;
		
		LuceneSchema lschema = null;
		Map<String,LuceneFieldSpec> mapLFld = new LinkedHashMap<>();
		AtomicReference<String> arKey = new AtomicReference<>();
		
		IOBiConsumer<String,Boolean> cField = (sqlKey,fPrimary)-> {
			LOG.info(String.format("Parsing %s sqlKey=%s", arKey.get(),sqlKey));
			String[] a = StringUtils.split(sqlKey, ",");
			for (String stFld : a) {
				TField field = mapField.get(stFld.toLowerCase());
				Objects.requireNonNull(field,()-> String.format("Key %s sqlKey=%s",stFld,sqlKey));
			
				if (!mapLFld.containsKey(field.getName())) {
					FieldType ft = field.getFieldType();
					LuceneFieldType lft = null;
					if (ft==FieldType.STRING || ft == FieldType.STRING_ID) {
						lft = fPrimary?LuceneFieldType.CASE_STRING:LuceneFieldType.STRING;
					} else if (ft== FieldType.INT) {
						lft = LuceneFieldType.INTEGER;
					} else if (ft == FieldType.LONG) {
						lft = LuceneFieldType.LONG;
					} else if (ft == FieldType.DATE) {
						lft = LuceneFieldType.POINT_DATE_HOUR;
					} else {
						lft = null;
					}
					if (lft != null) {
						LOG.info(String.format("Adding index for field %s type=%s", field.getName(),lft.canonicalName()));
						brbHash.appendChars(String.format("Index %s",lft.name()));
						LuceneFieldSpec lfld = new LuceneFieldSpec(field.getName(), lft, LuceneFieldSpec.LF_INDEXED);
						mapLFld.put(field.getName(), lfld);
					}
				}
			}
		};
		
		String stPrimaryKey = schema.getPrimaryKey();
		if (!StringUtils.isEmpty(stPrimaryKey)) {
			arKey.set("sqlPrimaryKey");
			cField.accept(stPrimaryKey, Boolean.TRUE);
		}
		
		String[] aUnique = schema.getUniqueKeys();
		if (aUnique != null) {
			arKey.set("sqlUniqueKey");
			for (String stFieldsUnique : aUnique) {
				cField.accept(stFieldsUnique, Boolean.FALSE);
			}
		}

		
		String[] aIndexes = schema.getIndexes();
		if (aIndexes != null) {
			arKey.set("sqlIndex");
			for (String stFieldsIndex : aIndexes) {
				cField.accept(stFieldsIndex, Boolean.FALSE);
			}
		}
		
		
		if (mapLFld.size()>0) {
			lschema = new LuceneSchema(schema.getName(), StandardBAnalyzer.NAME);
			for (LuceneFieldSpec lfld : mapLFld.values()) {
				lschema.addField(lfld);
			}
			LOG.info(String.format("Schema Created.\r\n%s\r\n", lschema.toString()));
			lschema.freeze();
		}
		
		MessageDigest md = cs.getMessageDigest();
		md.reset();
		md.update(brbHash.get().bytes,0,brbHash.length());
		byte[] aDigestText = md.digest();
		String stHash = PrintUtils.toHexString(aDigestText);
		if (cVersionHash != null) {
			cVersionHash.accept(version, stHash);
		}
		
		Set<TField> setField = new LinkedHashSet<>();
		setField.addAll(mapField.values());
		
		return new TSchema(tableName, version, SchemaType.REGULAR,setField, lschema);
		
	}
	
	public static TVault createTVault(File fileRoot,String versionPrefix) throws IOException {
		
		
		Validator.checkNewDir(fileRoot, true,true, "Root Dir of SQLDBSnap");
		TVault vault = TVault.open(FileVaultProvider.TYPE, fileRoot.getAbsolutePath(), Mode.CREATE,null);
		//Add All Schema ...
		Set<String> setName = SQLTableSchema.getAllTableSchemaNames();
		for (String stTable : setName) {
			SQLTableSchema schema = SQLTableSchema.get(stTable);
			updateTVault(vault,schema,versionPrefix);
		}
		vault.commitTOC();
		return vault;
		
	}
	
	
	
	
	
	public static void updateTVault(TVault vault, SQLTableSchema schema, String versionPrefix) throws IOException {
		Objects.requireNonNull(schema, "SQL schema");

		HashMap<String, String> prop = new HashMap<>();
		TSchema tschema = makeTSchema(schema, versionPrefix, (ver, hash) -> {
			prop.put("version", ver);
			prop.put("verHash", hash);
			prop.put("nameSQL", schema.getName());
			prop.put("primaryKeySQL", schema.getPrimaryKey());
			String stSeqName = schema.getSequenceName();
			if (stSeqName == null) {
				stSeqName = "";
			}
			prop.put("seqName", stSeqName);
		});

		String name = schema.getName();
		if (!vault.schemaExists(name)) {
			vault.addSchema(tschema);
		} else {
			TSchema tschemaPrev = vault.getSchema(name);
			String verHashPrev = tschemaPrev.getProps().get("verHash");
			String verHash = tschema.getProps().get("verHash");

			boolean fUpgrade = Strings.CI.compare(verHash, verHashPrev) != 0;
			LOG.info(String.format("Schema %s version=%s verHash=%s prevVersion=%s prevVerHash=%s fUpgrade=%s", name,
					tschema.getVersion(), verHash, tschemaPrev.getVersion(), verHashPrev, "" + fUpgrade));
			if (fUpgrade) {
				// Version upgrade...
				vault.updateSchema(tschema);
			}
		}
	}
	
	public static void addSnapShot(String stSQLDBName,File fileDirSnapShot,String versionPrefix) throws IOException {
		
		Validator.checkNewDir(fileDirSnapShot, true, true, "Snapshot dir");
		Set<String> setName = SQLTableSchema.getAllTableSchemaNames();
		for (String stTable : setName) {
			SQLTableSchema schema = SQLTableSchema.get(stTable);
			HashMap<String, String> prop = new HashMap<>();
			TSchema tschema = makeTSchema(schema, versionPrefix, (ver, hash) -> {
				prop.put("version", ver);
				prop.put("verHash", hash);
				prop.put("nameSQL", schema.getName());
				prop.put("primaryKeySQL", schema.getPrimaryKey());
				String stSeqName = schema.getSequenceName();
				if (stSeqName == null) {
					stSeqName = "";
				}
				prop.put("seqName", stSeqName);
			});
			File fileTableBee = new File(fileDirSnapShot, schema.getName()+".bee");
			LDirSuppliers ldSupp = new LDirSuppliers(fileTableBee.getAbsolutePath(),fileTableBee, null, true, Mode.CREATE);
			
			TSegmentBuilder b = new TSegmentBuilder(tschema, ldSupp);
			addAllRows(stSQLDBName,schema,b);
			b.commitWrite();
			ldSupp.close();
		}
	}
	
	public static void copyFields(SQLTableSchema ts,Decoder decoder,Encoder encoder) throws IOException {
		Objects.requireNonNull(ts);
		Objects.requireNonNull(decoder);
		Objects.requireNonNull(encoder);
		//call encoder.clear
		
		FieldType ft;
		String fn;
		
		for (SQLField field : ts.getSerializerFieldList()) {
			fn = field.getName();
			ft = field.getSerializerFieldType();
			//LOG.trace(String.format("copyFields %s %s(%d)", fn,ft.canonicalName(),ft.id()));
			switch(ft) {
			case FieldType.STRING:
			case FieldType.STRING_ID:
				encoder.writeString(fn,decoder.readString(fn));
				break;
				
			case FieldType.INT:
				encoder.writeInt(fn,decoder.readInt(fn));
				break;
				
			case FieldType.LONG:
				encoder.writeLong(fn,decoder.readLong(fn));
				break;
				
			case FieldType.BOOLEAN: 
				encoder.writeBoolean(fn,decoder.readBoolean(fn));
				break;
			
			case FieldType.FLOAT:
				encoder.writeFloat(fn,decoder.readFloat(fn));
				break;
			

			case FieldType.DOUBLE: 
				encoder.writeDouble(fn,decoder.readDouble(fn));
				break;
			


			case FieldType.BYTES: 
				encoder.writeBytes(fn, decoder.readBytes(fn));
				break;
			
			case FieldType.DATE: 
				encoder.writeDate(fn, decoder.readDate(fn));
				break;
				
			case FieldType.PROP:
				encoder.writeProperties(fn, decoder.readProperties(fn));
				break;
				
			case FieldType.INT_ARRAY:
				encoder.writeIntArray(fn, decoder.readIntArray(fn));
				break;
				
			case FieldType.LONG_ARRAY:
				encoder.writeLongArray(fn, decoder.readLongArray(fn));
				break;
			case FieldType.STRING_ARRAY:
				encoder.writeStringArray(fn, decoder.readStringArray(fn));
				break;

				
			default:
				throw new SolderException("Unsupported field type "+ft.canonicalName());
			
			}
		}
		
	}
	
	//First time (for small tables.)
	public static void addAllRows(String stSQLDBName,SQLTableSchema ts,TSegmentBuilder b) throws IOException{
		stSQLDBName = Validator.require(stSQLDBName,"sql db name",Rules.NO_NULL_EMPTY);
		Objects.requireNonNull(ts,"SQL table schema");
		Objects.requireNonNull(b,"SegBuilder");
		LOG.info(String.format("Populating TVault with schama %s from database %s",ts.getName(),stSQLDBName));
		
		SQLDatabase db = SQLDatabase.get(stSQLDBName);
		Objects.requireNonNull(db,"DB "+stSQLDBName);
		
		SQLQuery qAll = DriverUtil.createSelectQuery(stSQLDBName, db.getType(), ts, null, "All");
		LOG.info(String.format("Generate query:\r\n%s\r\n",qAll.getQuery()));
		
		String stSQLDBNameFinal = stSQLDBName;
		
		
		SQLTm.get().select(qAll,null, (decoder) -> {
			TRecordEncoder encoder = b.getEncoder();
			while (decoder.next()) {
				encoder.clear();
				copyFields(ts,decoder,encoder);
				b.add(encoder);
				if ( (b.size())%100 ==0) {
					LOG.info(String.format("Added %d Rows to %s",b.size(),stSQLDBNameFinal));
				}
			}
		}, null);
		
		LOG.info(String.format("Done adding %d Rows to %s",b.size(),stSQLDBNameFinal));
		
	}
	
	
	SQLDBSnap() {
		
	}
	
	
}
