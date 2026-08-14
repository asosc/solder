package org.solder.rest.solder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.rest.solder.SolderEntry.EntryType;

import com.beech.bfs.BeechFS;
import com.beech.bfs.Mode;
import com.ee.rest.RestException;
import com.jnk.util.CompareUtils;
import com.jnk.util.PrintUtils;
import com.jnk.util.Validator;
import com.lnk.lucene.LBytesRefBuilder;
import com.lnk.lucene.util.LogJsonDecoder;

public class RemoteRepoSync {
	
	
	private static Log LOG = LogFactory.getLog(RemoteRepoSync.class.getName());
	
	public static final String DEFAULT_SOLDER_IGNORE_FILE = ".solderignore";
	public static final String DEFAULT_SOLDER_DIR = ".solder";

	

	/** Sample size for sparse digests (first/last/middle chunks). */
	public static final int SPARSE_SAMPLE_SIZE = 4096;
	/** Cap on middle-region samples for large files. */
	public static final int SPARSE_MAX_MIDDLE_SAMPLES = 8;

	/**
	 * Fast sparse/sample digest for change detection (not for integrity).
	 * <p>
	 * Hashes file length, first 4KiB, last 4KiB, and one or more
	 * deterministically spaced 4KiB samples from the middle region.
	 * Small files ({@code <= 8KiB}) are hashed in full.
	 * <p>
	 * Currently uses SHA-256 over the samples only (no Guava/Murmur dependency).
	 * A project-owned fast hash utility can replace the hasher later; keep the
	 * sampling layout stable if digests are ever persisted across upgrades.
	 * Do not use this as a substitute for {@link #computeDigest(File)} where
	 * byte-exact integrity is required.
	 */
	public static String computeSparseDigest(File file) throws IOException {
		Objects.requireNonNull(file, "file");
		Validator.checkFile(file, "file");

		final int sample = SPARSE_SAMPLE_SIZE;
		long size = file.length();

		MessageDigest md = SolderEntry.tlMessageDigest.get();
		md.reset();
		// Length is part of the fingerprint so size changes always differ.
		md.update(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(size).array());

		if (size <= 0L) {
			return PrintUtils.toHexString(md.digest());
		}

		try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
			byte[] buf = new byte[sample];

			if (size <= (long) sample * 2L) {
				// Small file: first+last would overlap; hash entire content.
				int remaining = (int) size;
				while (remaining > 0) {
					int n = raf.read(buf, 0, Math.min(sample, remaining));
					if (n < 0) {
						throw new RestException("Unexpected EOF reading " + file.getAbsolutePath());
					}
					md.update(buf, 0, n);
					remaining -= n;
				}
				return PrintUtils.toHexString(md.digest());
			}

			// First 4KiB
			raf.seek(0L);
			raf.readFully(buf);
			md.update(buf);

			// Last 4KiB
			raf.seek(size - sample);
			raf.readFully(buf);
			md.update(buf);

			// Middle region [sample, size - sample)
			long midStart = sample;
			long midEnd = size - sample;
			long midLen = midEnd - midStart;

			if (midLen > 0L) {
				int nSamples;
				if (midLen <= sample) {
					nSamples = 1;
				} else {
					nSamples = (int) Math.min(SPARSE_MAX_MIDDLE_SAMPLES, midLen / sample);
					if (nSamples < 1) {
						nSamples = 1;
					}
				}

				for (int i = 0; i < nSamples; i++) {
					long offset;
					long readLen = Math.min(sample, midLen);
					if (nSamples == 1) {
						// Center the sample window in the middle region.
						offset = midStart + (midLen - readLen) / 2L;
					} else {
						// Evenly space sample starts across the middle region.
						offset = midStart + (i * (midLen - sample)) / (nSamples - 1);
					}
					if (offset < midStart) {
						offset = midStart;
					}
					if (offset + readLen > midEnd) {
						offset = midEnd - readLen;
					}
					int toRead = (int) readLen;
					raf.seek(offset);
					raf.readFully(buf, 0, toRead);
					md.update(buf, 0, toRead);
				}
			}
		}

		return PrintUtils.toHexString(md.digest());
	}
	
	
	
	//Remote objects will call server.

	
	

	public static void repoCheckout(SLocalRepo lrepo,SCommitInfo commitInfo,IRepoFileService rfs) throws IOException {
		// All New mean we cannot have any clash of relpaths in the directory.

		// tmp Dir wil
		Objects.requireNonNull(rfs,"Repo File Service");
		
		//Get the latest commit..
		SRepoInfo srepo = lrepo.getRepoInfo();
		SCommitInfo scommit =  null;
		
		int commitId;
		if (commitInfo == null) {
			// null means checkout repo tip (latest).
			commitId = srepo.getCommitId();
			int lcommitId = lrepo.getCommitId();
			LOG.info(String.format("Repo %s(%d) has commit; tip=%d (date=%s) local=%d", srepo.getId(),srepo.getSeqId(),commitId,
					PrintUtils.print(srepo.getCommitDate()),lcommitId));
			if (commitId <=0) {
				//Nothing to do..
				//Blank repo.
				return;
			}
			scommit =  rfs.getLatestCommit(srepo);
		} else {
			if (commitInfo.getRepoSeqId()!=srepo.getSeqId()) {
				throw new RestException(String.format("CommitInfo repo id mismatch; commit %d repoid %d; expect %d",commitInfo.getId(),commitInfo.getRepoSeqId(),srepo.getSeqId()));
			}
			commitId = commitInfo.getId();
			scommit = commitInfo;
		}
		
		
		
		Objects.requireNonNull(scommit);
		
		LOG.info(String.format("Checkout(rebase/clone) commit %d(hash=%s) for rep=%s",scommit.getId(),scommit.getCHash(),srepo.getId()));
		
		
		//We always download this..
		File fileDownload = rfs.downloadFile(srepo,"",scommit.getBlobFsId(),null);
		
		BeechFS fsCommit = new BeechFS(fileDownload, Mode.READONLY);
		try {
			checkoutFromCommit(lrepo, rfs, srepo, scommit,fsCommit);
		} finally {
			IOUtils.closeQuietly(fsCommit);
		}
	}

	static void checkoutFromCommit(SLocalRepo lrepo, IRepoFileService rfs, SRepoInfo srepo,SCommitInfo scommit, BeechFS fsCommit)
			throws IOException {
		
		//Get fS d
		Objects.requireNonNull(scommit,"scommit");
		Objects.requireNonNull(fsCommit,"fsCommit");
		
		InputStream is = fsCommit.read(CommitDetails.COMMIT_DETAIL);
		LBytesRefBuilder brb = new LBytesRefBuilder();
		try {
			brb.append(is, -1, true);
		} finally {
			IOUtils.closeQuietly(is);
		}
		String stJson = brb.get().utf8ToString();
		CommitDetails commitDetails = LogJsonDecoder.getTL().readObject(stJson, CommitDetails.class);
		
		//Check commitInfo matches with scommit..
		if (!commitDetails.getCHash().equals(scommit.getCHash()) || commitDetails.getCommitId() != scommit.getId()) {
			throw new RestException(
					String.format("CommitDetails Info mismatch with given SCommitInfo object. Req=(%d,%s) info=(%d,%s)",
							commitDetails.getCommitId(), commitDetails.getCHash(), scommit.getId(), scommit.getCHash()));
		}		
		
		
		//Careful with overwriting etc. (first copy all new additions)
		//Sync the commits..
		//do the deletions and then the orphans if asked to...
		
		Map<String,SolderEntry> mapCommit = new LinkedHashMap<>();
		mapCommit.putAll(commitDetails.getAllEntryMap());
		
		Map<String,SolderEntry> mapEntriesNow = lrepo.createEntryMap(mapCommit);
		
		MessageDigest md = SolderEntry.tlMessageDigest.get();
		for (var iter = mapCommit.values().iterator();iter.hasNext();iter.hasNext()) {
			SolderEntry seData = iter.next();
			
			if (seData.etype!=EntryType.BLOB) {
				//Non blobs(or commits) are dealt in the next loop.
				continue;
			}
			iter.remove();
			
			String stDataRelPath = seData.getRelPath();
			SolderEntry.requireSafeRelPath(stDataRelPath);
			
			
			
			//
			SolderEntry seCurrent= mapEntriesNow.remove(stDataRelPath);
			boolean fFetch = true;
			if (seCurrent!= null) {
				//Probably a previous abort brought it..
				//Check the hashes.
				if (CompareUtils.stringEquals(seCurrent.digest, seData.digest)) {
					//We have it with correct digest.. Nothing to do..
					LOG.info(String.format("Add %s (Exists with matching digest %s, Nothing to do.",stDataRelPath,seData.digest));
					fFetch=false;
					if (seCurrent.tModified != seData.tModified) {
						seCurrent.file.setLastModified(seData.tModified);
						// Local only; seData is server/commit metadata — do not mutate it.
						seCurrent.tModified = seCurrent.file.lastModified();
					}
				} else {
					LOG.info(String.format("Add %s (Exists with non-matching curr(sz=%d;digest=%s) add=(sz=%d, digest=%s), delete and refetch.",stDataRelPath,seCurrent.size,seCurrent.digest,seData.size,seData.digest));
					seCurrent.file.delete();
					if (seCurrent.file.exists()) {
						throw new RestException("Unable to delete existing file "+seCurrent.file.getAbsolutePath());
					}
				}
			}
			if (fFetch) {
				long fsId = seData.getBlobFsId();
				File fileSrc = rfs.downloadFile(srepo,stDataRelPath,fsId,seData.getDigest());
				File fileDest = lrepo.relPath.resolve(stDataRelPath);
				Validator.checkNewFile(fileDest,true, stDataRelPath);
				md.reset();
				InputStream isSrc = null;
				DigestOutputStream dos = null;
				try {
					isSrc = new FileInputStream(fileSrc);
					dos = new DigestOutputStream(new FileOutputStream(fileDest), md);
					IOUtils.copy(isSrc,dos);
				} finally {
					IOUtils.closeQuietly(dos, isSrc);
				}
				
				String stDigestWritten = PrintUtils.toHexString(md.digest());
				LOG.info(String.format("New Blob File %s",seData.toString()));
				
				if (!CompareUtils.stringEquals(seData.digest, stDigestWritten)) {
					throw new RestException("Digest match erorr for "+stDataRelPath+"; writtenDigest="+stDigestWritten+"; expect="+seData.digest);
				}
				fileDest.setLastModified(seData.getLastModified());
			}
		}
		
		//Sync the Commits...
		for (var iter = mapCommit.values().iterator();iter.hasNext();) {
			SolderEntry seCommit = iter.next();
			iter.remove();
			String stRelPath = seCommit.getRelPath();
			SolderEntry.requireSafeRelPath(stRelPath);
			SolderEntry seCurrent= mapEntriesNow.remove(stRelPath);
			boolean fCopy = true;
			if (seCurrent!= null) {
				//Probably a previous abort brought it..
				//Check the hashes.
				if (CompareUtils.stringEquals(seCurrent.digest, seCommit.digest)) {
					//We have it with correct digest.. Nothing to do..
					LOG.info(String.format("Add %s (Exists with matching digest %s, Nothing to do.",stRelPath,seCommit.digest));
					fCopy=false;
					if (seCurrent.tModified != seCommit.tModified) {
						seCurrent.file.setLastModified(seCommit.tModified);
						// Local only; seCommit is server/commit metadata — do not mutate it.
						seCurrent.tModified = seCurrent.file.lastModified();
					}

				} else {
					LOG.info(String.format("Commit %s (Exists with non-matching curr(sz=%d;digest=%s) add=(sz=%d, digest=%s), delete and refetch.",stRelPath,seCurrent.size,seCurrent.digest,seCommit.size,seCommit.digest));
					seCurrent.file.delete();
					if (seCurrent.file.exists()) {
						throw new RestException("Unable to delete existing file "+seCurrent.file.getAbsolutePath());
					}
				}
			}
			
			if (fCopy) {
				
				File fileDest = lrepo.relPath.resolve(stRelPath);
				Validator.checkNewFile(fileDest,true, stRelPath);
				md.reset();
				InputStream isSrc = null;
				DigestOutputStream dos = null;
				try {
					isSrc = fsCommit.read(stRelPath);
					dos = new DigestOutputStream(new FileOutputStream(fileDest), md);
					IOUtils.copy(isSrc,dos);
				} finally {
					IOUtils.closeQuietly(dos, isSrc);
				}
				
				String stDigestWritten = PrintUtils.toHexString(md.digest());
				LOG.info(String.format("New Commit File %s",seCommit.toString()));
				if (!CompareUtils.stringEquals(seCommit.digest, stDigestWritten)) {
					throw new RestException("Digest match erorr for "+stRelPath+"; writtenDigest="+stDigestWritten+"; expect="+seCommit.digest);
				}
				fileDest.setLastModified(seCommit.tModified);
			}
		}
		
		
		//Sync the Deletes..
		for (var iter = mapEntriesNow.values().iterator();iter.hasNext();) {
			//Delete 
			SolderEntry se = iter.next();
			iter.remove();
			LOG.info(String.format("REMOVING extra %s ",se.getRelPath()));
			se.file.delete();
		}
		
		//Set the Local File...
		// BeechLCommit lcommit;

		lrepo.commitId = commitDetails.commitId;
		lrepo.chash = commitDetails.cHash;
		lrepo.mapEntry = commitDetails.getAllEntryMap();
		lrepo.commitLocalRepo(false);
		

	}
	
	
	public static CommitDetails repCommit(SLocalRepo lRepo, File fileCache, Consumer<Map<String, String>> cCommitProp,IRepoFileService rfs)
			throws IOException {

		// SVault svault;
		// public SVault(String id,String schemaName,int tenantId,int aoId) throws
		// IOException {
		Objects.requireNonNull(rfs,"Repo File Service");

		SRepoInfo srepo = lRepo.srepo;
		CommitDetails commitDetails = new CommitDetails(lRepo,rfs);
		String stFileCache = fileCache!=null?fileCache.getAbsolutePath():"null";
		LOG.info(String.format("RemoteRepoSync repCommit %s from fileCache %s fCommitNew=%s commit=%d chash=%s", srepo.getId(),
				stFileCache, "" + commitDetails.fNewCommit, commitDetails.getCommitId(), commitDetails.cHash));

		if (!commitDetails.fNewCommit) {
			LOG.info(String.format("repCommit found no new changes, Nothing to do prev commit=%d chash=%s",commitDetails.getCommitId(),commitDetails.cHash));
			return commitDetails;
		}
		
		int idPrev=0;
		//Server will fill this based on the commitId.
		String chashPrev = null;
		
		if (srepo.getCommitId()>0) {
			idPrev = srepo.getCommitId();
		}
		
		// We may want to check if anything change at all...
		Map<String, String> mapCommitInfo = new LinkedHashMap<>();
		mapCommitInfo.put("commit", "some message");

		if (cCommitProp != null) {
			cCommitProp.accept(mapCommitInfo);
		}
		
		SCommitInfo sci = new SCommitInfo(-1,srepo.getSeqId(),commitDetails.getCHash(),idPrev,chashPrev,srepo.getTenantId(),-1L,new Date(),mapCommitInfo) ;
		
		CommitSession commitSess = rfs.beginCommit(sci,commitDetails.listMod,commitDetails.listDel);
		commitDetails.setCommitId(commitSess.getCommitId());
		
		for (String stUploadRelPath : commitDetails.listDedupUpload) {
			// Create a new item...
			SolderEntry se = commitDetails.mapAll.get(stUploadRelPath);
			Objects.requireNonNull(se,()->stUploadRelPath);
			long blobFsId = rfs.uploadFile(commitSess, se);
			if (blobFsId<=0) {
				throw new RestException("Invalid blobFsId "+blobFsId+" for path "+stUploadRelPath);
			}
			se.setBlobFsId(blobFsId);
		}
		
		commitDetails.setAndVerifyPostUpload();

		// Actual Commit file..
		// fileCommit
		commitDetails.finalizeFsCommit();
		
		SCommitInfo sciServer = rfs.uploadCommit(commitSess,commitDetails.getFileCommit());
		
		if (sciServer.getId()!=commitSess.commitId) {
			throw new RestException(String.format("Returned commitId %d is different from id %d assigned at beginCommit ", sciServer.getId(),commitSess.commitId));
		}
		
		if (!CompareUtils.stringEquals(sci.getCHash(),commitDetails.cHash)) {
			throw new RestException(String.format("Returned commitHash %s is different from %s provided by CommitDetails ", sci.getCHash(),commitDetails.cHash));
		}

		// Do the actual deletions by changing ownere..
		// Use to delete
		// Update your local REP....
		// BeechLCommit lcommit;

		lRepo.commitId = sciServer.getId();
		lRepo.chash = sciServer.getCHash();
		lRepo.mapEntry = commitDetails.getAllEntryMap();
		lRepo.commitLocalRepo(false);
		return commitDetails;

	}

}

