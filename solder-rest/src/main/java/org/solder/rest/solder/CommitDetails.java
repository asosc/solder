package org.solder.rest.solder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOConsumer;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.rest.solder.SolderEntry.EntryType;

import com.beech.bfs.BFile;
import com.beech.bfs.BeechFS;
import com.beech.bfs.Mode;
import com.ee.rest.RestException;
import com.jnk.util.CompareUtils;
import com.jnk.util.MapBuilder;
import com.jnk.util.PrintUtils;
import com.jnk.util.StopWatchUtil;
import com.jnk.util.Validator;
import com.lnk.lucene.TempFiles;
import com.lnk.lucene.util.LogJsonEncoder;
import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;
import com.lnk.serializer.ISerializable;


public class CommitDetails implements ISerializable {
	
	private static Log LOG = LogFactory.getLog(CommitDetails.class.getName());
	
	public static final String COMMITS_BEE = "Commits.bee";
	public static final String COMMIT_DETAIL = "_SCommitDetail";

	// Will be finally constructed
	String cHash;
	Map<String, SolderEntry> mapAll;
	
	Map<String, SolderEntry> mapDigestToFirst;
	List<String> listMod, listDel,listDedupUpload;

	int commitId;

	StringBuilder sbHash;
	BeechFS fsCommit = null;
	File fileTmpDir;
	File fileCommit;

	boolean fNewCommit;

	public CommitDetails() {
	}
	public File getFileCommit() {
		return fileCommit;
	}

	CommitDetails(SLocalRepo lRepo,IRepoFileService rfs) throws IOException {
		Objects.requireNonNull(rfs,"rfs");
		
		//Make sure we can even commit...
		SRepoInfo srepo = lRepo.srepo;
		Objects.requireNonNull(srepo,"srepo");
		
		//Refresh just before we commit.
		rfs.refresh(srepo);
		
		
		//Match ids first..
		Consumer<MapBuilder> cEventMb = (mb)->{
			mb.put("repo_id", srepo.getId());
			mb.put("srepo_commit_id",srepo.getCommitId());
			mb.put("lrepo_commit_id",lRepo.commitId);
			mb.put("lrepo_chash", lRepo.chash);
		};
		
		StopWatch sw = new StopWatch("CommitInfo");
		StopWatch swEntryMap = StopWatchUtil.makeSwatch("entryMap");
		StopWatch swCommitHash = StopWatchUtil.makeSwatch("commitHash");
		
		sw.start();
		
		
		sbHash = new StringBuilder();
		//Put the commit Id 
		

		Map<String, SolderEntry> mapSolderEntry = new LinkedHashMap<>();
		mapSolderEntry.putAll(lRepo.mapEntry);
		
		LOG.info(String.format("mapSolderEntry %d entries",mapSolderEntry.size()));
		
		swEntryMap.resume();

		Map<String, SolderEntry> mapEntriesNow = lRepo.createEntryMap(mapSolderEntry);
		swEntryMap.suspend();

		TempFiles tempFiles = TempFiles.get(TempFiles.DEFAULT);
		String repIdLog = lRepo.srepo.getId();
		if (repIdLog.length() > 6) {
			repIdLog = repIdLog.substring(0, 6);
		}
		fileTmpDir = tempFiles.getTempDir(repIdLog);
		Validator.checkDir(fileTmpDir, true, "temp dir");

		fileCommit = new File(fileTmpDir, COMMITS_BEE);
		fsCommit = new BeechFS(fileCommit, Mode.CREATE);

		mapAll = new TreeMap<>();
		listMod = new ArrayList<>();
		listDel = new ArrayList<>();
		
		
		mapDigestToFirst = new HashMap<String,SolderEntry>();
		listDedupUpload = new ArrayList<>();

		commitId = -1;

		IOConsumer<SolderEntry> cHashBuilder = (se) -> {
			LOG.info(String.format("Adding %s digest=%s", se.getRelPath(),se.getDigest()));
			var prev = mapAll.put(se.getRelPath(), se);
			if (prev != null) {
				throw new RestException("Found a prev entry for " + se.getRelPath());
			}
			sbHash.append(String.format("%s %s\r\n", se.stRelPath, se.digest));
		};
		
		swCommitHash.resume();

		for (var entry : mapEntriesNow.entrySet()) {
			String relPath = entry.getKey();
			SolderEntry se = entry.getValue();
			if (se.etype == EntryType.COMMIT) {
				InputStream is = null;
				OutputStream os = null;
				try {
					is = new FileInputStream(se.file);
					os = fsCommit.create(se.stRelPath);
					BFile bfile = fsCommit.getEntry(se.stRelPath);
					bfile.setTime(System.currentTimeMillis(), se.file.lastModified());
					IOUtils.copy(is, os);
				} finally {
					IOUtils.closeQuietly(os, is);
				}
				cHashBuilder.accept(se);

			} else if (se.etype == EntryType.BLOB) {

				// Either we have it or not...
				SolderEntry sePrev = mapSolderEntry.remove(relPath);
				if (sePrev != null && CompareUtils.stringEquals(sePrev.getDigest(),se.getDigest())) {
					// We have it.. (No change)
					cHashBuilder.accept(sePrev);
				} else {
					sePrev=null;
					// New File...
					se.setCommitId(commitId);
					cHashBuilder.accept(se);
					listMod.add(se.getRelPath());
				}
				
				
				SolderEntry seFirst = mapDigestToFirst.get(se.getDigest());
				if (seFirst == null) {
					seFirst = sePrev!=null?sePrev:se;
					if (!seFirst.getDigest().equals(se.getDigest())) {
						throw new RestException("Error is setting digest "+se.getRelPath());
					}
					mapDigestToFirst.put(seFirst.getDigest(), seFirst);
					LOG.info(String.format("Relpath %s first for %s",seFirst.getRelPath(),seFirst.getDigest()));
				}
				if (seFirst.getBlobFsId()<=0  && sePrev != null && sePrev.getBlobFsId()>0) {
					//We have the content...
					LOG.info(String.format("Found previous blobFSId %d for %s (digest=%s) donor=%s", sePrev.getBlobFsId(),seFirst.getRelPath(),seFirst.getDigest(),se.getRelPath()));
					seFirst.setBlobFsId(sePrev.getBlobFsId());
				}
			} else {
				throw new RestException("Unknown type " + se.etype);
			}

		}

		for (SolderEntry seDel : mapSolderEntry.values()) {
			if (seDel.etype == EntryType.BLOB) {
				LOG.info(String.format("Previous known %s deleted",seDel.stRelPath));
				listDel.add(seDel.getRelPath());
				sbHash.append(String.format("%s %s\r\n", seDel.stRelPath, "DELETE"));
			}
		}

		byte[] aBHashBytes = sbHash.toString().getBytes(StandardCharsets.UTF_8);
		swCommitHash.suspend();

		MessageDigest md = SolderEntry.tlMessageDigest.get();
		md.reset();
		md.update(aBHashBytes);
		cHash = PrintUtils.toHexString(md.digest());
		fNewCommit = !CompareUtils.stringEquals(lRepo.chash, cHash);
		
		sw.suspend();
		
		
		//Calculate UploadList
//		/listDedupUpload
		long szUpload = 0L;
		for (Map.Entry<String,SolderEntry> entry: mapDigestToFirst.entrySet()) {
			
			String digest = entry.getKey();
			SolderEntry se = entry.getValue();
			if (se.getBlobFsId()<=0) {
				//Need to upload...
				//Cannot dedup with our knowledge (send it to server for upload).
				listDedupUpload.add(se.getRelPath());
				szUpload += se.size;
				if (!mapAll.containsKey(se.getRelPath())) {
					throw new RestException(String.format("New content relpath %s not found in mapAll digest=%s",se.getRelPath(),digest));
				}
			}
		}
		
		

		LOG.info(String.format("Commit %s**(fNewCommit=%s)\r\n%s\rnHash=%s (lRepoHash=%s) (nMod=%d,nDel=%d) (nUpload=%d,szUpload=%,d)",
				lRepo.srepo.getId(), "" + fNewCommit, sbHash, cHash, lRepo.chash, listMod.size(), listDel.size(),listDedupUpload.size(),szUpload));
		StopWatchUtil.printTime("CommitInfo Times", sw, swEntryMap,swCommitHash);

		
	}
	
	void setCommitId(int commitIdNew) {
		if (commitIdNew<=0) {
			throw new RuntimeException("Invalid commitId "+commitIdNew);
		}
		commitId = commitIdNew;
		for (String st : listMod) {
			var se = mapAll.get(st);
			se.setCommitId(commitId);
		}
		
	}
	
	void setAndVerifyPostUpload() throws IOException{
		
		
		for (var entry : mapAll.entrySet()) {
			String relPath = entry.getKey();
			SolderEntry se = entry.getValue();
			if (se.etype == EntryType.BLOB) {
				if (se.getBlobFsId()<=0) {
					// Map is digest-keyed (first path that owns the upload for that content).
					SolderEntry seFirst = mapDigestToFirst.get(se.getDigest());
					Objects.requireNonNull(seFirst, () -> "first entry for digest " + se.getDigest() + " path=" + relPath);
					if (seFirst.getBlobFsId()<=0) {
						boolean f = listDedupUpload.contains(seFirst.getRelPath());
						throw new RestException(String.format("SolderEntry %s has no valid blobFsId; digest=%s fInDedupList=%s",seFirst,seFirst.getDigest(),Boolean.toString(f)));
					}
					se.setBlobFsId(seFirst.getBlobFsId());
				}
			}
		}
		
	}
	
	public void finalizeFsCommit() throws IOException {
		if (fNewCommit) {
			OutputStream os = fsCommit.create(COMMIT_DETAIL);
			LogJsonEncoder.getTL().serialize(this, (br) -> {
				os.write(br.bytes, 0, br.length);
			});
			os.close();
			fsCommit.commit();
		} else {
			//Dont bother with commiting etc.. Just close...
			LOG.info(String.format("No changes deteced; Not a new Commit. "));
		}
		
		fsCommit.close();
		fsCommit=null;
	}

	public void serialize(Encoder encoder) throws IOException {
		encoder.writeInt("commit_id", commitId);
		encoder.writeString("chash", cHash);
		//se_add is really se_mod.. No need to change the internal store variable.
		encoder.writeObjectArray("se_all", mapAll.values().toArray(SolderEntry.EMPTY_SOLDER_ENTRY), false);
		encoder.writeStringArray("se_add", listMod.toArray(PrintUtils.EMPTY_STRING_ARRAY));
		encoder.writeStringArray("se_del", listDel.toArray(PrintUtils.EMPTY_STRING_ARRAY));
	}

	public static List<String> toList(String[] a) {
		List<String> list = new ArrayList<>();
		if (a !=null) {
			for (String st : a) {
				list.add(st);
			}
		}
		return list;
	}
	
	public void deserialize(Decoder decoder) throws IOException {
		commitId = decoder.readInt("commit_id");
		cHash = decoder.readString("chash");

		SolderEntry[] aAll = decoder.readObjectArray("se_all", SolderEntry.class);
		mapAll = new LinkedHashMap<>();
		if (aAll!=null) {
			for (var se : aAll) {
				mapAll.put(se.getRelPath(), se);
			}
		}
		listMod = toList(decoder.readStringArray("se_add"));
		listDel = toList(decoder.readStringArray("se_del"));
	}
	
	public boolean isNewCommit() {
		return fNewCommit;
	}

	public int getCommitId() {
		return commitId;
	}

	public String getCHash() {
		return cHash;
	}

	
	public List<String> getAllModified() {
		return listMod;
	}

	public List<String> getAllDeleted() {
		return listDel;
	}

	public Map<String, SolderEntry> getAllEntryMap() {
		return mapAll;
	}
	
}