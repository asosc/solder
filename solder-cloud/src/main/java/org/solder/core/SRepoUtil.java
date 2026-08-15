package org.solder.core;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nimbo.blobs.BlobFS;
import org.nimbo.blobs.BlobFile;
import org.nimbo.blobs.Container;
import org.nimbo.blobs.NimboException;
import org.solder.rest.solder.CommitDetails;
import org.solder.rest.solder.SolderEntry;
import org.solder.rest.solder.SolderEntry.EntryType;

import com.beech.bfs.BeechFS;
import com.beech.bfs.Mode;
import com.jnk.util.PrintUtils;
import com.jnk.util.Validator;
import com.lnk.lucene.LBytesRefBuilder;
import com.lnk.lucene.util.LogJsonDecoder;

public class SRepoUtil {

	private static Log LOG = LogFactory.getLog(SRepoUtil.class.getName());

	private static final String[] USAGE_HEADER = new String[] { "sid", "commitId", "RelPath", "blobFsId", "size",
			"sizeCharged", "commitCharged", "RepoCharged", "orphanCharged" };

	static enum UEType {
		COMMIT, DATA, ORPHAN;
	}

	static class UsageEntry {
		int sid, commitId;
		long blobFSId;
		BlobFS blobFS;
		// The first entry that uses this blobs is charged
		// Rest take a free ride.
		boolean fCharged;
		long szResolved;

		UEType type;
		SolderEntry se;

		UsageEntry(int sid, int commitId, UEType type, long blobFSId, BlobFS blobFS, boolean fCharged) {
			this.sid = sid;
			this.commitId = commitId;
			this.type = Objects.requireNonNull(type);
			this.blobFSId = blobFSId;
			this.blobFS = blobFS;
			this.fCharged = blobFS != null && fCharged;
			this.szResolved = 0;
		}

		void setSolderEntry(SolderEntry se) {
			this.se = se;
			if (se != null && se.getSize() > 0) {
				this.szResolved = se.getSize();
			}

		}

	}

	public static class SRepoUsage {
		SRepo srepo;
		List<SCommit> listCommit;
		Map<Long, BlobFS> mapBlobFSRepo, mapBlobFSCommit;

		List<BlobFS> listBlobFSRepo, listBlobFSCommit;

		Set<Integer> setCommitIdError;

		long szRepoCharged, szRepoOrphan;

		List<UsageEntry> listUE;
		
		

		public SRepoUsage(SRepo srepo) throws IOException {
			this.srepo = Objects.requireNonNull(srepo);
			listCommit = srepo.getAllCommit();

			mapBlobFSRepo = new LinkedHashMap<>();
			mapBlobFSCommit = new LinkedHashMap<>();

			listBlobFSRepo = BlobFS.selectByOwner(SRepo.BLOB_TYPE_SOLDER_REPO, "" + srepo.getSeqId());
			listBlobFSCommit = BlobFS.selectByOwner(SRepo.BLOB_TYPE_SOLDER_COMMIT, "" + srepo.getSeqId());

			Map<Long, BlobFS> mapBlobFSRepo2 = new LinkedHashMap<>();
			Map<Long, BlobFS> mapBlobFSCommit2 = new LinkedHashMap<>();

			for (BlobFS blobFS : listBlobFSCommit) {
				mapBlobFSCommit.put(blobFS.getId(), blobFS);
				mapBlobFSCommit2.put(blobFS.getId(), blobFS);
			}
			for (BlobFS blobFS : listBlobFSRepo) {
				mapBlobFSRepo.put(blobFS.getId(), blobFS);
				mapBlobFSRepo2.put(blobFS.getId(), blobFS);
			}

			List<SCommit> listBadCommit = new ArrayList<>();
			int nCommitFileError = 0;
			setCommitIdError = new HashSet<>();
			Set<Long> setBlobFSidError = new HashSet<>();

			int sid = srepo.getSeqId();

			listUE = new ArrayList<>();

			for (int i = listCommit.size() - 1; i >= 0; i--) {
				SCommit commit = listCommit.get(i);
				int commitId = commit.getId();

				long blobCommitFsId = commit.getBlobFsId();
				BlobFS blobFS = mapBlobFSCommit2.remove(blobCommitFsId);
				UsageEntry ue = new UsageEntry(sid, commitId, UEType.COMMIT, blobCommitFsId, blobFS, true);
				listUE.add(ue);

				LOG.info(String.format("#Commit %d blobFsId=%d fBlobFound=%s", commitId, blobCommitFsId,
						Boolean.toString(blobFS != null)));
				// csvPrinter.printComment(sb.toString());

				if (blobFS == null) {
					listBadCommit.add(commit);
					setCommitIdError.add(commit.getId());
					LOG.info(String.format("commit %d; cannot find blobFSId %d (nBadCommits=%d)", commit.getId(),
							blobCommitFsId, listBadCommit.size()));
					continue;
				}

				BeechFS fsCommit = null;
				try {
					File fileCommit = srepo.downloadFile("", blobCommitFsId, null);
					ue.szResolved = fileCommit.length();

					fsCommit = new BeechFS(fileCommit, Mode.READONLY);

					InputStream is = fsCommit.read(CommitDetails.COMMIT_DETAIL);
					LBytesRefBuilder brb = new LBytesRefBuilder();
					try {
						brb.append(is, -1, true);
					} finally {
						IOUtils.closeQuietly(is);
					}
					String stJson = brb.get().utf8ToString();
					CommitDetails commitDetails = LogJsonDecoder.getTL().readObject(stJson, CommitDetails.class);

					// Check commitInfo matches with scommit..
					boolean fCommitFileError = !commitDetails.getCHash().equals(commit.getCHash())
							|| commitDetails.getCommitId() != commit.getId();

					if (fCommitFileError) {
						setCommitIdError.add(commit.getId());
						nCommitFileError++;
						LOG.info(String.format(
								"Commit %d (nCommitFileError=%d) CommitDetail Info mismatch with given SCommitInfo object. Req=(%d,%s) info=(%d,%s)",
								commit.getId(), nCommitFileError, commitDetails.getCommitId(), commitDetails.getCHash(),
								commit.getId(), commit.getCHash()));
						continue;
					}

					for (SolderEntry se : commitDetails.getAllEntryMap().values()) {
						if (se.getType() != EntryType.BLOB) {
							// Non blobs(or commits) are dealt in the next loop.
							continue;
						}

						long seBlobFsId = se.getBlobFsId();
						BlobFS blobSe = mapBlobFSRepo.get(seBlobFsId);
						boolean fCharged = false;
						if (blobSe == null) {
							setBlobFSidError.add(seBlobFsId);
						} else {
							fCharged = mapBlobFSRepo2.remove(seBlobFsId) != null;
						}
						ue = new UsageEntry(sid, commitId, UEType.DATA, seBlobFsId, blobSe, fCharged);
						ue.setSolderEntry(se);
						listUE.add(ue);
					}

				} catch (Exception e) {
					setCommitIdError.add(commit.getId());
					LOG.info(String.format("Error while analyzing Commit %d", commit.getId()), e);
				} finally {
					IOUtils.closeQuietly(fsCommit);
				}

			}

			// File blobs not referenced by any successfully scanned commit package.
			for (BlobFS blobFS : mapBlobFSRepo2.values()) {
				UsageEntry ue = new UsageEntry(sid, blobFS.getExtId(), UEType.ORPHAN, blobFS.getId(), blobFS, true);
				ue.szResolved = resolveBlobSize(blobFS);
				listUE.add(ue);

			}

			// Commit-package blobs with no matching SCommit row (e.g. tip CAS lost after
			// blob commit, or abandoned upload session).
			for (BlobFS blobFS : mapBlobFSCommit2.values()) {
				UsageEntry ue = new UsageEntry(sid, blobFS.getExtId(), UEType.ORPHAN, blobFS.getId(), blobFS,
						true);
				ue.szResolved = resolveBlobSize(blobFS);
				listUE.add(ue);
			}

			szRepoCharged = 0L;
			szRepoOrphan = 0L;
			for (UsageEntry ue : listUE) {
				long sz = Math.max(ue.szResolved, 0);
				long szCharged = ue.fCharged ? sz : 0L;
				szRepoCharged += szCharged;
				if (ue.type == UEType.ORPHAN) {
					szRepoOrphan += szCharged;
				}

			}

		}
		
		public SRepo getRepo() {
			return srepo;
		}
		
		public List<SCommit> getAllCommit() {
			return listCommit;
		}
		
		public Set<Integer> getCommitIdsWithError() {
			return setCommitIdError;
		}
		
		public List<UsageEntry> getAllEntry() {
			return listUE;
		}
		
		public long getChargedSize() {
			return szRepoCharged;
		}
		
		public long getOrphanSize() {
			return szRepoOrphan;
		}
		
		public void doRemoveOrphan(boolean fDryRun) throws IOException {
			// Live content is defined by tip (and typically tip's prev snap). If either failed to
			// scan, orphan classification is untrustworthy — block reclaim. Otherwise reclaim
			// all orphans (otherwise they stay orphan forever).
			if (isTipOrTipPrevScanError()) {
				String msg = String.format(
						"doRemoveOrphan blocked; tip and/or tip prev had commit scan errors. tip=%d tipPrev=%d errorCommits=%s fDryRun=%s",
						srepo.getCommitId(), tipPrevCommitId(), StringUtils.join(setCommitIdError, ","),
						Boolean.toString(fDryRun));
				LOG.info(msg);
				throw new SolderException(msg);
			}
			deleteChargedBlobs(fDryRun, true);
		}

		private int tipPrevCommitId() {
			int tip = srepo.getCommitId();
			if (tip <= 0) {
				return 0;
			}
			for (SCommit c : listCommit) {
				if (c.getId() == tip) {
					return c.getPrevId();
				}
			}
			return 0;
		}

		private boolean isTipOrTipPrevScanError() {
			int tip = srepo.getCommitId();
			if (tip > 0 && setCommitIdError.contains(tip)) {
				return true;
			}
			int tipPrev = tipPrevCommitId();
			return tipPrev > 0 && setCommitIdError.contains(tipPrev);
		}
		
		public void purgeRepo(boolean fDryRun) throws IOException {
			if (!srepo.fDeleted ) {
				throw new SolderException(String.format("Purge can be called on Repo already marked for deletion! repo %s(%d)",srepo.getSeqId(),srepo.getId()));
			}
			
			for (SCommit commit : listCommit) {
				//Delete..
				if (!fDryRun) {
					commit.delete();
				}
			}

			// Delete all charged blobs (commit packages, file data, and orphans).
			deleteChargedBlobs(fDryRun, false);
		}

		/**
		 * @param fOrphansOnly if true, only ORPHAN* entries; if false, all charged blobs (purge).
		 */
		private void deleteChargedBlobs(boolean fDryRun, boolean fOrphansOnly) throws IOException {
			int nConsecError = 0;
			for (UsageEntry ue : listUE) {
				if (fOrphansOnly && ue.type != UEType.ORPHAN) {
					continue;
				}
				if (!ue.fCharged || ue.blobFS == null) {
					continue;
				}
				String relPath = relPathForCsv(ue);
				long sz = Math.max(ue.szResolved, 0);
				LOG.info(String.format("Found blob to delete type=%s path=%s blob=%d sz=%d fDryRun=%s",
						ue.type.name(), relPath, ue.blobFSId, sz, Boolean.toString(fDryRun)));
				if (fDryRun) {
					continue;
				}
				try {
					Container.delete(ue.blobFS, true);
					nConsecError = 0;
				} catch (Exception e) {
					nConsecError++;
					if (nConsecError > 3) {
						throw NimboException.rethrow(e);
					}
					LOG.info(String.format("Ignore Error deleting nConsec=%d blobFSID=%d type=%s", nConsecError,
							ue.blobFSId, ue.type.name()), e);
				}
			}
		}
		

		public void getUsageCsv(File fileRepoReportRoot) throws IOException {

			LOG.info(String.format("Usage report for repo %s(%d) isDeleted=%s", srepo.getId(), srepo.getSeqId(),
					Boolean.toString(srepo.fDeleted)));

			StringBuilder sb = new StringBuilder();
			sb.append(String.format("#Repo sid=%d id=%s nCommits=%d tCreate=%s tLastCommit=%s\r\n", srepo.getSeqId(),
					srepo.getId(), listCommit.size(), PrintUtils.print(srepo.getCreateDate()),
					PrintUtils.print(srepo.getCommitDate())));

			File fileRepoUsage = new File(fileRepoReportRoot, srepo.getSeqId() + "_usage.csv");
			Validator.checkNewFile(fileRepoUsage, true, "Repo usage file");

			StringBuilder sbCsv = new StringBuilder();
			CSVPrinter csvPrinter = new CSVPrinter(sbCsv, CSVFormat.DEFAULT);

			csvPrinter.printRecord((Object[]) USAGE_HEADER);

			csvPrinter.printComment(sb.toString());

			List<Object> listVal = new ArrayList<>();

			long szRepoCharged = 0, szOrphan = 0, szCommitCharged = 0;
			int commitId = -1;
			for (UsageEntry ue : listUE) {

				if (commitId != ue.commitId) {
					szCommitCharged = 0L;
					commitId = ue.commitId;
				}

				if (ue.type == UEType.COMMIT) {
					sb.setLength(0);
					sb.append(String.format("#Commit %d blobFsId=%d fBlobFound=%s", ue.commitId, ue.blobFSId,
							Boolean.toString(ue.blobFS != null)));
					csvPrinter.printComment(sb.toString());
				}

				String relPath = relPathForCsv(ue);
				long sz = Math.max(ue.szResolved, 0);
				long szCharged = ue.fCharged ? sz : 0L;
				if (ue.type == UEType.COMMIT || ue.type == UEType.DATA) {
					szCommitCharged += szCharged;
				} else {
					szOrphan += szCharged;
				}
				szRepoCharged += szCharged;

				listVal.clear();
				listVal.add(ue.sid);
				listVal.add(ue.commitId);
				listVal.add(relPath);
				listVal.add(ue.blobFSId);
				listVal.add(sz);
				listVal.add(szCharged);
				listVal.add(szCommitCharged);
				listVal.add(szRepoCharged);
				listVal.add(szOrphan);
				csvPrinter.printRecord(listVal);

			}

			csvPrinter.close();

			String stCsv = sbCsv.toString();
			LOG.info(String.format("\r\n****** CSV Table of SRepo ***\r\n%s\r\n*********\r\n", stCsv));

			try (FileWriter w = new FileWriter(fileRepoUsage, StandardCharsets.UTF_8)) {
				w.write(stCsv);
				w.write("\r\n");
			}
		}

		private static String relPathForCsv(UsageEntry ue) {
			if (ue.type == UEType.COMMIT) {
				return "CommitFile";
			}
			if (ue.type == UEType.DATA) {
				return ue.se != null ? ue.se.getRelPath() : "Unknown";
			}
			// ORPHAN — label by blob owner for readability only.
			if (ue.blobFS != null && SRepo.BLOB_TYPE_SOLDER_COMMIT.equals(ue.blobFS.getOwnerApp())) {
				return "OrphanCommit:CommitFile";
			}
			String path = null;
			if (ue.blobFS != null && ue.blobFS.getInfo() != null) {
				path = ue.blobFS.getInfo().get("path");
			}
			if (StringUtils.isEmpty(path)) {
				path = "Unknown";
			}
			return "OrphanFile:" + path;
		}

		private static long resolveBlobSize(BlobFS blobFS) {
			long sz = blobFS.getSize();
			if (sz > 0) {
				return sz;
			}
			try {
				BlobFile blobFile = Container.read(blobFS);
				return blobFile.getFile().length();
			} catch (Exception e) {
				LOG.info(String.format("Error getting blob %d", blobFS.getId()), e);
				return -1;
			}
		}

	}
	
	public static long[] getDeletedRepoUsage(File fileRepoReportRoot) throws IOException {
		return getAllRepoUsage(SRepo.getDeletedRepo(), fileRepoReportRoot);
	}

	public static long[] getAllRepoUsage(File fileRepoReportRoot) throws IOException {
		return getAllRepoUsage(SRepo.getAll(), fileRepoReportRoot);
	}

	public static long[] getAllRepoUsage(List<SRepo> listRepo, File fileRepoReportRoot) throws IOException {
		Objects.requireNonNull(listRepo);
		long szTotal = 0, szOrphan = 0L;
		for (SRepo repo : listRepo) {
			SRepoUsage repoUsage = new SRepoUsage(repo);
			szTotal += repoUsage.szRepoCharged;
			szOrphan += repoUsage.szRepoOrphan;
			repoUsage.getUsageCsv(fileRepoReportRoot);
		}

		LOG.info(String.format("Final RepoUsage nRepos=%d szCharged=%,d bytes, szOrphan=%,d", listRepo.size(),
				szTotal, szOrphan));

		return new long[] { szTotal, szOrphan };
	}

}
