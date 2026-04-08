package org.solder.rest.client;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.ee.rest.RestException;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;
import com.lnk.serializer.ISerializable;

public class SCommitInfo implements ISerializable, Comparable<SCommitInfo> {
	protected int id,idPrev;

	protected String chash, repoId,chashPrev;
	protected long blobFsId;
	protected int tenantId;
	protected Date dateCreate;
	protected Map<String, String> mapInfo;

	public SCommitInfo() {
	}

	public SCommitInfo(SRepoInfo repo, String chash, Map<String, String> mapInfo, int commitId) throws IOException {
		// Generated from sequence.
		if (commitId <= 0 || commitId <= repo.getCommitId()) {
			throw new RestException("Invalid commitId " + commitId);
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
			chashPrev = chash;
		}

		dateCreate = new Date();
		if (mapInfo == null) {
			mapInfo = new LinkedHashMap<>();
		}
		this.mapInfo = mapInfo;
	}

	public int compareTo(SCommitInfo sc) {
		// Natural order is based on id.
		return id - sc.id;
	}

	public boolean equals(Object o) {
		if (o instanceof SCommitInfo sc) {
			return id == sc.id;
		} else {
			return false;
		}
	}
	
	

	public void serialize(Encoder encoder) throws IOException {
		encoder.writeInt("id", id);
		encoder.writeString("repo_id", repoId);
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
		repoId = decoder.readString("repo_id");
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

	public String getRepoId() {
		return repoId;
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