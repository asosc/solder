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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nimbo.blobs.BlobFS;
import org.nimbo.blobs.BlobFile;
import org.nimbo.blobs.Container;
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
	
	public static long getUsage(SRepo srepo,File fileRepoReportRoot) throws IOException {
		
		LOG.info(String.format("Usage report for repo %s(%d) isDeleted=%s", srepo.getId(),srepo.getSeqId(),Boolean.toString(srepo.fDeleted)));
		
		List<SCommit> listCommit = srepo.getAllCommit();
		
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("#Repo sid=%d id=%s nCommits=%d tCreate=%s tLastCommit=%s\r\n", srepo.getSeqId(),srepo.getId(),listCommit.size(),
				PrintUtils.print(srepo.getCreateDate()),PrintUtils.print(srepo.getCommitDate())));
		
		
		
		File fileRepoUsage = new File(fileRepoReportRoot,srepo.getSeqId()+".usage");
		Validator.checkNewFile(fileRepoUsage, true, "Repo usage file");
		
		
		StringBuilder sbCsv = new StringBuilder();
		CSVPrinter csvPrinter = new CSVPrinter(sbCsv,CSVFormat.DEFAULT);
		
		
		
		String[] aStHeader = new String[] {"sid","commitId","RelPath","blobFsId","size","sizeCharged","commitCharged","RepoCharged"};
		
		csvPrinter.printRecord((Object[])aStHeader);
		
		csvPrinter.printComment(sb.toString());
		
		List<Object> listVal = new ArrayList<>();
		
		Map<Long,BlobFS> mapBlobFSRepo = new LinkedHashMap<>(); 
		Map<Long,BlobFS> mapBlobFSCommit = new LinkedHashMap<>();
		Map<Long,BlobFS> mapBlobFSRepo2 = new LinkedHashMap<>(); 
		Map<Long,BlobFS> mapBlobFSCommit2 = new LinkedHashMap<>();
		
		List<BlobFS> listBlobFSRepo = BlobFS.selectByOwner(SRepo.BLOB_TYPE_SOLDER_REPO,""+srepo.getSeqId());
		List<BlobFS> listBlobFSCommit = BlobFS.selectByOwner(SRepo.BLOB_TYPE_SOLDER_COMMIT,""+srepo.getSeqId());
		
		
		for (BlobFS blobFS : listBlobFSCommit) {
			mapBlobFSCommit.put(blobFS.getId(), blobFS);
			mapBlobFSCommit2.put(blobFS.getId(), blobFS);
		}
		for (BlobFS blobFS : listBlobFSRepo) {
			mapBlobFSRepo.put(blobFS.getId(), blobFS);
			mapBlobFSRepo2.put(blobFS.getId(), blobFS);
		}	
		
		int szRepoCharged=0,szOrphan=0;
		
		List<SCommit> listBadCommit = new ArrayList<>();
		int nCommitFileError=0;
		Set<Integer> setCommitIdError = new HashSet<>();
		Set<Long> setBlobFSidError = new HashSet<>();
		
		int sid = srepo.getSeqId();
		for (int i=listCommit.size()-1;i>=0;i--) {
			
			int szCommitCharged=0;
			//Latest comes first..
			SCommit commit = listCommit.get(i);
			int commitId = commit.getId();
		
			listVal.clear();
			
			//Get the commit...
			long blobCommitFsId = commit.getBlobFsId();
			BlobFS blobFS =mapBlobFSCommit2.remove(blobCommitFsId);
			
			sb.setLength(0);
			sb.append(String.format("#Commit %d blobFsId=%d fBlobFound=%s", commitId,blobCommitFsId,Boolean.toString(blobFS!=null)));
			csvPrinter.printComment(sb.toString());
			
		
			if (blobFS == null) {
				listBadCommit.add(commit);
				setCommitIdError.add(commit.getId());
				LOG.info(String.format("commit %d; cannot find blobFSId %d (nBadCommits=%d)", commit.getId(),blobCommitFsId,listBadCommit.size()));
				continue;
			}
			
			
			
			BeechFS fsCommit=null;
			try {
				File fileCommit = srepo.downloadFile("",blobCommitFsId,null);
				long szCommitFile  = fileCommit.length();
				szCommitCharged += szCommitFile;
				szRepoCharged += szCommitFile;
				listVal.clear();
				listVal.add(sid);
				listVal.add(commitId);
				listVal.add("CommitFile");
				listVal.add(blobCommitFsId);
				listVal.add(szCommitFile);
				listVal.add(szCommitFile);
				listVal.add(szCommitCharged);
				listVal.add(szRepoCharged);
				listVal.add(szOrphan);
				csvPrinter.printRecord(listVal);
				
				
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
				
				//Check commitInfo matches with scommit..
				boolean fCommitFileError = !commitDetails.getCHash().equals(commit.getCHash()) || commitDetails.getCommitId() != commit.getId();
				
				if (fCommitFileError) {
					setCommitIdError.add(commit.getId());
					nCommitFileError++;
					LOG.info(String.format("Commit %d (nCommitFileError=%d) CommitDetail Info mismatch with given SCommitInfo object. Req=(%d,%s) info=(%d,%s)",
									commit.getId(),nCommitFileError,commitDetails.getCommitId(), commitDetails.getCHash(), commit.getId(), commit.getCHash()));
					continue;
				}		
				
				
				
				for (SolderEntry se : commitDetails.getAllEntryMap().values()) {
					if (se.getType()!=EntryType.BLOB) {
						//Non blobs(or commits) are dealt in the next loop.
						continue;
					}
					
					long seBlobFsId = se.getBlobFsId();
					BlobFS blobSe = mapBlobFSRepo.get(seBlobFsId);
					if (blobSe==null) {
						setBlobFSidError.add(seBlobFsId);
						
					}
					
					long sz = Math.max(0, se.getSize());
					long szCharged = 0;
					if (blobSe!=null) {
						if (mapBlobFSRepo2.remove(seBlobFsId)!=null) {
							szCharged=sz;
						}
					}
					
					szCommitCharged += szCharged;
					szRepoCharged += szCharged;
					listVal.clear();
					listVal.add(sid);
					listVal.add(commitId);
					listVal.add(se.getRelPath());
					listVal.add(seBlobFsId);
					listVal.add(sz);
					listVal.add(szCharged);
					listVal.add(szCommitCharged);
					listVal.add(szRepoCharged);
					listVal.add(szOrphan);
					csvPrinter.printRecord(listVal);
				}
				
				
			}catch(Exception e) {
				setCommitIdError.add(commit.getId());
				LOG.info(String.format("Error while analyzing Commit %d",commit.getId()),e);
			}finally {
				IOUtils.closeQuietly(fsCommit);
			}
			
			
			//Print Orphans.
			
		}
		
		for (BlobFS blobFS : mapBlobFSRepo2.values()) {
			
			String relPath = blobFS.getInfo().get("path");
			if (relPath==null) {
				relPath="Unknown";
			}
			
			long sz = blobFS.getSize();
			if (sz<=0) {
				//Old or not set..
				try {
					BlobFile blobFile = Container.read(blobFS);
					File file = blobFile.getFile();
					sz = file.length();
					
				}catch(Exception e) {
					LOG.info(String.format("Error getting blob %d", blobFS.getId(),e));
					sz  = -1;
				}
			}
			if (sz>0) {
				szOrphan+=sz;
				szRepoCharged+=sz;
			}
			
			listVal.clear();
			listVal.add(sid);
			listVal.add(blobFS.getExtId());
			listVal.add("relPath");
			listVal.add(blobFS.getId());
			listVal.add(sz);
			listVal.add(sz);
			listVal.add(0);
			listVal.add(szRepoCharged);
			listVal.add(szOrphan);
			csvPrinter.printRecord(listVal);
		}
		
		csvPrinter.close();
		
		String stCsv = sb.toString();
		LOG.info(String.format("\r\n****** CSV Table of SRepo ***\r\n%s\r\n*********\r\n",stCsv));
		
		try (FileWriter w = new FileWriter(fileRepoUsage,StandardCharsets.UTF_8)) {
			w.write(stCsv);
			w.write("\r\n");
		}
		
		return szRepoCharged;
		
	}
	
	
	public static long getDeletedRepoUsage(SRepo srepo,File fileRepoReportRoot) throws IOException {
		return getAllRepoUsage( SRepo.getDeletedRepo(),fileRepoReportRoot);		
	}
	
	public static long getAllRepoUsage(SRepo srepo,File fileRepoReportRoot) throws IOException {
		return getAllRepoUsage( SRepo.getAll(),fileRepoReportRoot);		
	}
	
	
	public static long getAllRepoUsage(List<SRepo> listRepo,File fileRepoReportRoot) throws IOException {
		Objects.requireNonNull(listRepo);
		long szTotal =0;
		for (SRepo repo : listRepo) {
			szTotal += getUsage(repo,fileRepoReportRoot);
		}
		return szTotal;
	}

}
