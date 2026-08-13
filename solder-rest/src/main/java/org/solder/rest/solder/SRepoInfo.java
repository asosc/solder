package org.solder.rest.solder;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

import com.ee.rest.RestException;
import com.lnk.lucene.record.RecordUtil;
import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;
import com.lnk.serializer.ISerializable;

public class SRepoInfo implements ISerializable  {
	
	protected int sid;
	protected String id, tschema, tag,commitDir;
	protected int tenantId, aoId, commitId;
	protected Date dateCommit, dateChange, dateCreate, dateUpdate;
	
	//transient - Server uses it to attach itself to an object.
	transient Object parent;

	public SRepoInfo() {
	}

	
	public SRepoInfo(int sid, String id, String tschema, int tenantId, int aoId,String tag,String commitDir, int commitId,
			Date dateCommit, Date dateChange, Date dateCreate, Date dateUpdate) {
		//tschema - is mainly type,schema and other attributes that app wants to group on.
		//aoId - object instance id along with tenant_id makes it unique. 
		
		//CommitDir is special for databases. All files here must be small and possibly changing. Files
		//here are package into containers and stored each  time anyone of the file changed.
		this.sid=sid;
		this.id=id;
		this.tschema = tschema;
		this.tenantId=tenantId;
		this.aoId = aoId;
		this.tag = tag;
		this.commitDir = commitDir;
		this.commitId = commitId;
		this.dateCommit = dateCommit;
		this.dateChange = dateChange;
		this.dateCreate=dateCreate;
		this.dateUpdate = dateUpdate;
	}
	

	public void serialize(Encoder encoder) throws IOException {
		encoder.writeInt("sid", sid);
		encoder.writeString("id", id);
		encoder.writeString("tschema", tschema);
		encoder.writeInt("tenant_id", tenantId);
		encoder.writeInt("ao_id", aoId);
		encoder.writeString("tag", tag);
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
		tschema = decoder.readString("tschema");
		tenantId = decoder.readInt("tenant_id");
		aoId = decoder.readInt("ao_id");
		tag = decoder.readString("tag");
		commitDir = decoder.readString("commit_dir");
		commitId = decoder.readInt("commit_id");
		dateCommit = decoder.readDate("commit_date");
		dateChange = decoder.readDate("change_date");
		dateCreate = decoder.readDate("create_date");
		dateUpdate = decoder.readDate("last_update");
	}
	
	public void setParent(Object srepo) {
		this.parent =srepo;
	}
	
	public Object getParent() {
		return parent;
	}
	
	
	public int getSeqId() {
		return sid;
	}
	
	public String getId() {
		return id;
	}

	public String getName() {
		return getId();
	}

	public String getSchemaName() {
		return tschema;
	}

	public int getTenantId() {
		return tenantId;
	}

	public int getAoId() {
		return aoId;
	}
	
	public String getTag() {
		return tag;
	}
	
	public String getCommitDir() {
		return commitDir;
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
	
	public String toString() {
		return RecordUtil.printJson(this, false);
	}
	
	public void refresh(SRepoInfo repoRefresh) throws IOException {
		Objects.requireNonNull(repoRefresh,"repo refresh");
		
		
		if (!repoRefresh.getId().equals(id) || repoRefresh.getTenantId() != tenantId || repoRefresh.getSeqId()!=sid) {
			throw new RestException("Error refreshing, obj mismatch");
		}
	
		tag = repoRefresh.tag;
		commitId=repoRefresh.commitId;
		dateCommit = repoRefresh.dateCommit;
		dateChange = repoRefresh.dateChange;
		dateUpdate = repoRefresh.dateUpdate;
		
		
	}	
	
}