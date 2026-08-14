package org.solder.rest.solder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import com.ee.rest.RestException;
import com.jnk.util.CompareUtils;
import com.jnk.util.PrintUtils;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;import com.lnk.lucene.BitUtil;
import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;
import com.lnk.serializer.ISerializable;

public class SolderEntry implements ISerializable {
	
	
	static final SolderEntry[] EMPTY_SOLDER_ENTRY = new SolderEntry[0];
	
	public enum EntryType {
		BLOB(1), COMMIT(2);

		int type;

		EntryType(int type) {
			this.type = type;
		}

	}

	public static EntryType getEntryTypeEnum(int type) {
		if (type == EntryType.BLOB.type) {
			return EntryType.BLOB;
		} else if (type == EntryType.COMMIT.type) {
			return EntryType.COMMIT;
		} else {
			return null;
		}
	}

	/**
	 * Reject absolute paths and path traversal in relative repo paths.
	 */
	public static void requireSafeRelPath(String relPath) throws IOException {
		Validator.require(relPath, "rel path", Rules.NO_NULL_EMPTY);
		String normalized = relPath.replace('\\', '/');
		if (normalized.startsWith("/") || normalized.indexOf(':') >= 0) {
			throw new RestException("Illegal rel path: " + relPath);
		}
		for (String part : normalized.split("/")) {
			if (part.isEmpty() || ".".equals(part)) {
				continue;
			}
			if ("..".equals(part)) {
				throw new RestException("Illegal rel path: " + relPath);
			}
		}
	}
	
	public static final ThreadLocal<MessageDigest>  tlMessageDigest = ThreadLocal.withInitial(() -> {
		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (Exception e) {
			throw RestException.rethrowUnchecked(e);
		}
	});

	public static String computeDigest(File file) throws IOException {

		MessageDigest md = tlMessageDigest.get();
		md.reset();
		DigestOutputStream dos = new DigestOutputStream(NullOutputStream.INSTANCE, md);
		InputStream is = new FileInputStream(file);
		try {
			IOUtils.copy(is, dos);
			byte[] digest = md.digest();
			return PrintUtils.toHexString(digest);
		} finally {
			IOUtils.closeQuietly(dos, is);
		}

	}

	
	String stRelPath;
	EntryType etype;
	long tModified, size;
	String digest;
	long blobFsId;
	int commitId;

	// Transient States.
	File file;

	public SolderEntry() {
	}
	
	void verifyPrev(SolderEntry sePrev,File file) throws IOException {
		if (sePrev != null) {
			if (!CompareUtils.stringEquals(stRelPath, sePrev.stRelPath)) {
				throw new RestException(
						String.format("RelPath mismatch for %s (expect %s)", stRelPath, sePrev.getRelPath()));
			}
			if (etype != sePrev.etype) {
				throw new RestException(
						String.format("Type mismatch for %s; got %s (expect %s)", stRelPath,etype.name(), sePrev.getType().name()));
			}
			
			if (sePrev.size != file.length() || sePrev.tModified!=file.lastModified()) {
				throw new RestException(
						String.format("File attribute mismatch %s got (size=%d,lastMod=%d) expect (size=%d,lastMod=%d) ",stRelPath,file.length(),file.lastModified(),sePrev.size,sePrev.tModified));
			}
		}
	}

	public SolderEntry(String relPath, EntryType etype, File file, long blobFsId, int commitId,SolderEntry sePrev) throws IOException {
		requireSafeRelPath(relPath);
		Objects.requireNonNull(etype);

		Objects.requireNonNull(file, "file");
		this.stRelPath = relPath;
		this.etype = etype;
		verifyPrev(sePrev,file);
		
		this.tModified = file.lastModified();
		this.size = file.length();
		this.digest = sePrev==null?computeDigest(file):sePrev.digest;
		this.blobFsId = blobFsId;
		this.commitId = commitId;

		// For building commits;
		this.file = file;
	}

	public void setBlobFsId(long blobFsId) {
		this.blobFsId = blobFsId;
	}

	public void setCommitId(int commitId) {
		this.commitId = commitId;
	}
	
	

	public void serialize(Encoder encoder) throws IOException {
		encoder.writeString("path", stRelPath);
		encoder.writeInt("type", etype.type);
		encoder.writeLong("mod", tModified);
		encoder.writeLong("sz", size);
		encoder.writeString("digest", digest);
		encoder.writeLong("blob_fsid", blobFsId);
		encoder.writeLong("commit_id", commitId);

	}

	public void deserialize(Decoder decoder) throws IOException {
		stRelPath = decoder.readString("path");
		etype = getEntryTypeEnum(decoder.readInt("type"));
		tModified = decoder.readLong("mod");
		size = decoder.readLong("sz");
		digest = decoder.readString("digest");
		blobFsId = decoder.readLong("blob_fsid");
		commitId = decoder.readInt("commit_id");
	}

	public String getRelPath() {
		return stRelPath;
	}

	public EntryType getType() {
		return etype;
	}

	public long getLastModified() {
		return tModified;
	}

	public long getSize() {
		return size;
	}

	public String getDigest() {
		return digest;
	}

	public long getBlobFsId() {
		return blobFsId;
	}

	public int getCommitId() {
		return commitId;
	}
	
	//Transient
	public File getFile() {
		return file;
	}
	
	public void setFile(File file) {
		this.file=file;
	}

	public String toString() {
		return String.format("SolderEntry %s type=%s len=%s (digest=%s lastMod=%d blobFsId=%s commitId=%d)",
				stRelPath, "" + etype, size, digest, tModified, blobFsId, commitId);
	}
}