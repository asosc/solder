package org.solder.rest.solder;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;
import com.lnk.serializer.ISerializable;

public class SCommitInfo implements ISerializable, Comparable<SCommitInfo> {
	protected int id,idPrev,repoSid;

	protected String chash, chashPrev;
	protected long blobFsId;
	protected int tenantId;
	protected Date dateCreate;
	protected Map<String, String> mapInfo;
	
	//transient - Server uses it to attach itself to an object.
	transient Object parent;

	public SCommitInfo() {
	}
	
	
	public SCommitInfo(int id,int repoSid,String chash,int idPrev,String chashPrev,int tenantId,long blobFsId,Date dateCreate,Map<String,String> mapInfo)  {
		this.id = id;
		this.repoSid=repoSid;
		this.chash = chash;
		this.idPrev = idPrev;
		this.chashPrev = chashPrev;
		this.tenantId = tenantId;
		this.blobFsId = blobFsId;
		this.dateCreate = dateCreate;
		this.mapInfo = mapInfo;
	}

		
	public void setParent(Object scommit) {
		this.parent =scommit;
	}
	
	public Object getParent() {
		return parent;
	}

	public int compareTo(SCommitInfo sc) {
		// Natural order is based on id.
		if (id>0) {
			return Integer.compare(id, sc.id);
		} else {
			//does not happen.. as CommitInfo should be created with valid id..
			int diff = dateCreate.compareTo(sc.dateCreate);
			if (diff != 0) {
				return diff;
			} else {
				return chash.compareTo(sc.chash);
			}
		}
	}

	public boolean equals(Object o) {
		if (o instanceof SCommitInfo sc) {
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

	
}