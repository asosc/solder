package org.solder.rest.solder;

import java.io.IOException;
import java.util.Objects;

import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;
import com.lnk.serializer.ISerializable;

public class SUsageEntry implements ISerializable {
	public static enum SueType {
		COMMIT, DATA, ORPHAN;
	}

	int sid, commitId;
	long blobFsId;

	boolean fFoundBlobFS;

	// The first entry that uses this blobs is charged
	// Rest take a free ride.
	boolean fCharged;
	long sz;

	SueType type;
	String relPath;
	
	
	public SUsageEntry() {
		
	}

	public SUsageEntry(int sid, int commitId, SueType type, String relPath, long blobFSId, boolean fFoundBlobFS,
			long sz, boolean fCharged) {
		this.sid = sid;
		this.commitId = commitId;
		this.type = Objects.requireNonNull(type);
		this.relPath = relPath;
		this.blobFsId = blobFSId;
		this.fFoundBlobFS = fFoundBlobFS;
		this.sz = sz;
		this.fCharged = fCharged;
		
	}
	
	public void serialize(Encoder encoder) throws IOException {
		encoder.writeInt("sid", sid);
		encoder.writeInt("commit_id", commitId);
		encoder.writeString("type", type.name());
		encoder.writeString("path", relPath);
		encoder.writeLong("blob_fsid", blobFsId);
		encoder.writeBoolean("blob_found", fFoundBlobFS);
		encoder.writeLong("sz", sz);
		encoder.writeBoolean("charged", fCharged);
	}

	public void deserialize(Decoder decoder) throws IOException {
		sid = decoder.readInt("sid");
		commitId = decoder.readInt("commit_id");
		type = SueType.valueOf(decoder.readString("type"));
		relPath = decoder.readString("path");
		
		blobFsId = decoder.readLong("blob_fsid");
		fFoundBlobFS = decoder.readBoolean("blob_found");
		sz = decoder.readLong("sz");
		fCharged = decoder.readBoolean("charged");
	}

	public int getSid() {
		return sid;
	}

	public int getCommitId() {
		return commitId;
	}

	public long getBlobFsId() {
		return blobFsId;
	}

	public boolean isBlobFSFound() {
		return fFoundBlobFS;
	}

	public boolean isCharged() {
		return fCharged;
	}

	public long size() {
		return sz;
	}

	public SueType getType() {
		return type;
	}

	public String getRelPath() {
		return relPath;
	}
	


}
