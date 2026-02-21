package org.solder.rest.client;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;



import com.ee.rest.client.EnigmaRestObjects.ERemote;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.jnk.util.cache.CacheHelper;
import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;

public class SolderRestObjects {
	
	
	public static class RSRepo extends ERemote {
		
		String id, schemaName, commitDir;
		String[] aExtension;
		int tenantId, aoId, commitId;
		Date dateCommit, dateChange, dateCreate, dateUpdate;

		public RSRepo() {
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
		
		
	}

		
	

}
