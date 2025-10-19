package org.solder.vsync;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DelegateFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.function.IOConsumer;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nimbo.blobs.BlobFS;
import org.nimbo.blobs.BlobFileTransact;
import org.nimbo.blobs.ContainerGroup;
import org.solder.core.SolderException;
import org.solder.core.SolderMain;
import org.solder.vsync.SolderVaultFactory.SCommit;
import org.solder.vsync.SolderVaultFactory.SRepo;

import com.aura.crypto.CryptoScheme;
import com.beech.bfs.BFile;
import com.beech.bfs.BeechException;
import com.beech.bfs.BeechFS;
import com.beech.bfs.BeechLCommit;
import com.beech.bfs.BeechLDirectory;
import com.beech.bfs.Mode;
import com.beech.compress.BytesCompressor;
import com.ee.session.SessionManager;
import com.ee.util.LogJsonEncoder;
import com.jnk.util.CompareUtils;
import com.jnk.util.PrintUtils;
import com.jnk.util.RelPath;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.lnk.lucene.BytesRefInputStream;
import com.lnk.lucene.BytesRefOutputStream;
import com.lnk.lucene.LBytesRefBuilder;
import com.lnk.lucene.TempFiles;
import com.lnk.lucene.io.LDirectory;
import com.lnk.lucene.record.RecordUtil;
import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;
import com.lnk.serializer.ISerializable;
import com.lnk.serializer.JsonDecoder;
import com.lnk.serializer.JsonEncoder;

public class SolderRepoOps {

	private static Log LOG = LogFactory.getLog(SolderRepoOps.class.getName());

	static ThreadLocal<MessageDigest> tlMessageDigest = ThreadLocal.withInitial(() -> {
		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (Exception e) {
			throw SolderException.rethrowUnchecked(e);
		}
	});

	static String computeDigest(File file) throws IOException {

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
	
	public enum EntryType {
		BLOB(1),COMMIT(2);
		
		int type;
		EntryType(int type) {
			this.type=type;
		}
		
	}
	
	public static EntryType getEntryTypeEnum(int type) {
		if (type==EntryType.BLOB.type) {
			return EntryType.BLOB;
		} else if (type==EntryType.COMMIT.type) {
			return EntryType.COMMIT;
		} else {
			return null;
		}
	}

	public static class SolderEntry implements ISerializable {
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

		public SolderEntry(String relPath,EntryType etype, File file, long blobFsId, int commitId) throws IOException {
			Validator.require(relPath, "rel path", Rules.NO_NULL_EMPTY);
			Objects.requireNonNull(etype);
			
			Objects.requireNonNull(file, "file");
			this.stRelPath = relPath;
			this.etype=etype;
			this.tModified = file.lastModified();
			this.size = file.length();
			this.digest = computeDigest(file);
			this.blobFsId = blobFsId;
			this.commitId = commitId;
			
			//For building commits;
			this.file=file;
		}

		public void setBlobId(long blobFsId) {
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
		
		public String toString() {
			return String.format("SolderEntry %s type=%s len=%s (digest=%s lastMod=%d blobFsId=%s commitId=%d)",stRelPath,""+etype,size,digest,tModified,blobFsId,commitId);
		}
	}

	static final String SOLDER_LOCAL_DIR = ".solder";
	static final String SOLDER_LOCAL_REPO = "slrepo";
	static final String LDIR_ROOT = "sl";
	static final int LDIR_VERSION = 1;
	
	static final String BLOB_TYPE_SOLDER_COMMIT = "solder_commit";

	static String getLocalRepoCommitPath() {
		// .bee is intentionally taken out.
		return String.format("%s/%s", SOLDER_LOCAL_DIR, SOLDER_LOCAL_REPO);
	}

	static LDirectory getLDirectory(BeechFS fs, String dirRoot, Mode mode, IOConsumer<BeechFS> onClose)
			throws IOException {

		return new BeechLDirectory(fs, fs.getFileName(), dirRoot, mode, onClose);
	}

	public static class SLocalRepo {

		// BeechLCommit lcommit;
		String repoId;
		int commitId;
		String chash;

		File fileCommitLocalRepo;
		File fileDotSolder;
		Mode mode;
		BeechFS fs;
		LDirectory ldir;

		SRepo srepo;
		File fileRoot;
		RelPath relPath;

		Map<String, SolderEntry> mapEntry;
		String stCommitDirRelPath;
		Set<String> setExtensionsToAllow;
		boolean fDirty = false;
		
		BeechLCommit lcommit;

		LDirectory getLDirectory() throws IOException {
			if (ldir == null) {

				if (fs == null) {
					fs = new BeechFS(fileCommitLocalRepo, mode);
				}

				IOConsumer<BeechFS> onClose = (fsClose) -> {
					IOUtils.closeQuietly(fsClose);
					this.fs=null;
				};
				ldir = new BeechLDirectory(fs, fs.getFileName(), LDIR_ROOT, mode, onClose);

			}
			Objects.requireNonNull(ldir, "ldir");
			return ldir;
		}

		

		SLocalRepo(SRepo repo, File fileLocalRepoRoot)
				throws IOException {

			srepo = Objects.requireNonNull(repo, "repo");
			Validator.checkDir(fileLocalRepoRoot, false, "repo dir");
			this.fileRoot = fileLocalRepoRoot;
			relPath = new RelPath(this.fileRoot);
			stCommitDirRelPath = srepo.getCommitDir();
			if (stCommitDirRelPath == null) {
				stCommitDirRelPath = "";
			}
			this.stCommitDirRelPath = stCommitDirRelPath.trim();
			
			String[] aExtToKeep = repo.getExtensionToKeep();

			setExtensionsToAllow = new LinkedHashSet<>();
			if (aExtToKeep != null && aExtToKeep.length > 0) {
				for (String st : aExtToKeep) {
					setExtensionsToAllow.add(st.trim().toLowerCase());
				}
			}

			fileCommitLocalRepo = new File(fileLocalRepoRoot, getLocalRepoCommitPath());
			this.repoId = Validator.require(repo.getId(), "repo id", Rules.NO_NULL_EMPTY, Rules.TRIM_LOWER);
			// Will be read from file
			this.commitId = 0;
			this.chash = "";
			mapEntry = new LinkedHashMap<>();
			

			boolean fExist = fileCommitLocalRepo.exists();
			if (fExist) {
				mode = Mode.WRITE;
				this.loadLocalRepo();
			} else {
				mode = Mode.CREATE;
				Validator.checkDir(fileCommitLocalRepo.getParentFile(),true,"local repo");
				commitLocalRepo(true);
			}
		}

		static final SolderEntry[] EMPTY_ARRAY = new SolderEntry[0];

		private void commitLocalRepo(boolean fCreate) throws IOException {
			Mode.checkWrite(mode, SOLDER_LOCAL_REPO);

			if (lcommit==null) {
				if (fCreate) {
					lcommit = new BeechLCommit(SOLDER_LOCAL_REPO);
				} else {
					throw new SolderException("Unexpected Null lcommit");
				}
			}

			Objects.requireNonNull(lcommit, "lcommit");

			// Close the directory right after USE..
			// All interprocess locking is left to application.
			if (ldir == null) {
				ldir = getLDirectory();
			}

			SolderEntry[] aSolderEntry = mapEntry.values().toArray(EMPTY_ARRAY);
			try {
				lcommit.commit(ldir, (out) -> {
					out.writeVInt(LDIR_VERSION);
					out.writeString(repoId);
					out.writeString(stCommitDirRelPath);
					out.writeSetOfStrings(setExtensionsToAllow);
					out.writeInt(commitId);
					out.writeString(chash);
					JsonEncoder jsonEncoder = RecordUtil.getTLJsonEncoder();
					BytesRefOutputStream bros = new BytesRefOutputStream();
					jsonEncoder.reset(bros);
					JsonEncoder.serialize(jsonEncoder, () -> {
						jsonEncoder.writeObjectArray("entries", aSolderEntry, false);
					});

					BytesCompressor bc = new BytesCompressor();
					bc.setFastCompression(true);
					bc.encode(bros.getBytesRef(), out);

				});
			} finally {
				IOUtils.closeQuietly(ldir);
				ldir = null;
			}
			fDirty = false;
		}

		void loadLocalRepo() throws IOException {

			BeechLCommit lcommitNew = new BeechLCommit(SOLDER_LOCAL_REPO);

			// Close the directory right after USE..
			// All interprocess locking is left to application.

			try {
				if (ldir == null) {
					ldir = getLDirectory();
				}
				lcommitNew.load(ldir, (in) -> {
					int version = in.readVInt();
					if (version != LDIR_VERSION) {
						throw new BeechException("Unknown version " + version + "; expect=" + LDIR_VERSION);
					}
					String repoIdRead = in.readString();
					if (!CompareUtils.stringEquals(repoId, repoIdRead)) {
						throw new BeechException("Name mismatch; repoIdRead=" + repoIdRead + "; expect " + repoId);
					}

					stCommitDirRelPath = in.readString();
					setExtensionsToAllow = in.readSetOfStrings();
					commitId = in.readInt();
					chash = in.readString();

					BytesCompressor bc = new BytesCompressor();
					LBytesRefBuilder brbTemp = new LBytesRefBuilder();
					bc.decode(in, brbTemp);
					BytesRefInputStream bris = new BytesRefInputStream();
					bris.reset(brbTemp.get());

					JsonDecoder jsonDecoder = RecordUtil.getTLJsonDecoder();
					jsonDecoder.reset(bris);
					JsonDecoder.deserialize(jsonDecoder, () -> {
						SolderEntry[] aSolderEntry = jsonDecoder.readObjectArray("entries", SolderEntry.class);
						mapEntry = new LinkedHashMap<>();
						for (SolderEntry entry : aSolderEntry) {
							mapEntry.put(entry.getRelPath(), entry);
						}
					});

				});

			} finally {
				IOUtils.closeQuietly(ldir);
				ldir = null;
			}
			
			this.lcommit=lcommitNew;

			fDirty = false;

			// Spliterators.spliterator(aSegInfo,Spliterator.SIZED).
			LOG.debug(String.format("LocalRepo(%s)-> nEntry=%d; entryRelPath={%s}", repoId, mapEntry.size(),
					StringUtils.join(mapEntry.keySet(), ',')));
		}

		IOFileFilter getFileFilter() {

			if (setExtensionsToAllow == null || setExtensionsToAllow.size() == 0) {
				return TrueFileFilter.INSTANCE;
			} else {
				FileFilter filter = (file) -> {
					String ext = FilenameUtils.getExtension(file.getName());
					return ext != null && setExtensionsToAllow.contains(ext);
				};
				return new DelegateFileFilter(filter);
			}
		}

		public Collection<File> scan() {
			//Put directory Filter...
			FileFilter filter = (file) -> {
				LOG.info("Directory filter examine "+file.getAbsolutePath());
				String name = file.getName();
				return !CompareUtils.stringEquals(name, SOLDER_LOCAL_DIR);
			};
			IOFileFilter dirFilter =  new DelegateFileFilter(filter);
			
			return FileUtils.listFiles(fileRoot, getFileFilter(), dirFilter);
		}
		
		public Map<String,SolderEntry> createEntryMap() throws IOException {
			Map<String,SolderEntry> mapEntriesNow = new TreeMap<>();
			Collection<File> collFile = scan();
			
			String prefix = null;
			if (stCommitDirRelPath!=null && stCommitDirRelPath.length()>0) {
				prefix = stCommitDirRelPath+"/";
			}
			
			for (File file : collFile) {
				String path = relPath.relativize(file.getAbsolutePath());
				EntryType etype = (prefix==null || path.startsWith(prefix))?EntryType.COMMIT:EntryType.BLOB;
				SolderEntry se = new SolderEntry(path,etype,file,-1L,0);
				mapEntriesNow.put(path, se);
				LOG.info(String.format("Collecting file %s",""+se));
			}
			return mapEntriesNow;
		}

	}
	static final String COMMITS_BEE="Commits.bee";
	static final String COMMIT_DETAIL="_SCommitDetail";
	public static class CommitInfo  implements ISerializable {
		
		//Will be finally constructed
		String cHash;
		List<SolderEntry> listAll,listAdd,listDel;
		int commitId;
		
		
		StringBuilder sbHash;
		BeechFS fsCommit = null;
		File fileTmpDir;
		File fileCommit;
		
		boolean fNewCommit;
		
		public CommitInfo() {}
		
		CommitInfo(SLocalRepo lRepo) throws IOException{
			sbHash = new StringBuilder();
			
			
			Map<String, SolderEntry> mapSolderEntry = new LinkedHashMap<>();
			mapSolderEntry.putAll(lRepo.mapEntry);
			
			Map<String,SolderEntry> mapEntriesNow = lRepo.createEntryMap();
			
			TempFiles tempFiles = TempFiles.get(TempFiles.DEFAULT);
			fileTmpDir = tempFiles.getTempDir(lRepo.srepo.getId() + "_rep_" + System.currentTimeMillis());
			Validator.checkDir(fileTmpDir, true, "temp dir");

			
			
			fileCommit = new File(fileTmpDir, COMMITS_BEE);
			fsCommit = new BeechFS(fileCommit, Mode.CREATE);
			
			listAll = new ArrayList<>();
			listAdd = new ArrayList<>();
			listDel = new ArrayList<>();
			
			commitId = -1;

			IOConsumer<SolderEntry> cHashBuilder= (se)->{
				listAll.add(se);
				sbHash.append(String.format("%s %s\r\n",se.stRelPath,se.digest));
			};

			for (var entry : mapEntriesNow.entrySet()) {
				String relPath = entry.getKey();
				SolderEntry se = entry.getValue();
				if (se.etype == EntryType.COMMIT) {
					InputStream is = new FileInputStream(se.file);
					OutputStream os = fsCommit.create(se.stRelPath);
					BFile bfile = fsCommit.getEntry(se.stRelPath);
					bfile.setTime(System.currentTimeMillis(), se.file.lastModified());
					IOUtils.copy(is, os);
					os.close();
					cHashBuilder.accept(se);
					
				} else if (se.etype == EntryType.BLOB) {
					
					//Either we have it or not...
					SolderEntry sePrev = mapSolderEntry.remove(relPath);
					if (sePrev != null) {
						//We have it..
						cHashBuilder.accept(sePrev);
					} else {
						//New File...
						se.setCommitId(commitId);
						listAdd.add(se);
						cHashBuilder.accept(se);
					}
					
				} else {
					throw new SolderException("Unknown type "+se.etype);
				}
				
			}
			
			for (SolderEntry seDel : mapSolderEntry.values()) {
				if (seDel.etype==EntryType.BLOB) {
					listDel.add(seDel);
					sbHash.append(String.format("%s %s\r\n",seDel.stRelPath,"DELETE"));
				}
			}
			
			byte[] aBHashBytes = sbHash.toString().getBytes(StandardCharsets.UTF_8);
			
			MessageDigest md = tlMessageDigest.get();
			md.reset();
			md.update(aBHashBytes);
			cHash = PrintUtils.toHexString(md.digest());
			fNewCommit = !CompareUtils.stringEquals(lRepo.chash, cHash);
			
			LOG.info(String.format("Commit %s**(fNewCommit=%s)\r\n%s\rnHash=%s (lRepoHash=%s) (nAdd=%d,nDel=%d)", lRepo.srepo.getId(),""+fNewCommit,sbHash,cHash,lRepo.chash,listAdd.size(),listDel.size()));
			
			if (fNewCommit) {
				commitId = SolderVaultFactory.generateCommitId();
				for (SolderEntry se : listAdd) {
					se.setCommitId(commitId);
				}
				
				OutputStream os = fsCommit.create(COMMIT_DETAIL);
				LogJsonEncoder.getTL().serialize(this,(br)->{
					os.write(br.bytes,0,br.length);
				});
				os.close();
			}
			
			
			fsCommit.close();
			
			
		}
		
		public void serialize(Encoder encoder) throws IOException {
			encoder.writeInt("commit_id", commitId);
			encoder.writeString("chash", cHash);
			encoder.writeList("se_all", listAll,false);
			encoder.writeList("se_add", listAdd,false);
			encoder.writeList("se_del", listDel,false);
		}

		public void deserialize(Decoder decoder) throws IOException {
			commitId = decoder.readInt("commit_id");
			cHash = decoder.readString("chash");
			listAll = decoder.readList("se_all",SolderEntry.class);
			listAdd = decoder.readList("se_add",SolderEntry.class);
			listDel = decoder.readList("se_del",SolderEntry.class);
		}

		public int getCommitId() {
			return commitId;
		}
		
		public String getCHash() {
			return cHash;
		}
		
		public List<SolderEntry> getAllEntry() {
			return listAll;
		}

		public List<SolderEntry> getAddEntry() {
			return listAdd;
		}
		
		public List<SolderEntry> getDelEntry() {
			return listDel;
		}
		
		public Map<String, SolderEntry> makeLocalRepoMap() {
			Map<String, SolderEntry> map = new TreeMap<>();
			for (var se : listAll) {
				map.put(se.stRelPath, se);
			}
			return map;
		}
		
	}

	/**
	 * This function should be use if you have files for the repository already
	 * created and you have not made the files as part of the clone...
	 * 
	 * Will only work if there is NO .solder directory. This will first clone and
	 * Bring everything over (without overwriting existing files in the directory.
	 * 
	 * Commit can be called after.
	 * 
	 * @param srepo
	 * @param fileCache
	 * @throws IOException
	 */
	public static void repInit(SRepo srepo, File fileCache)
			throws IOException {
		
		SLocalRepo lrepo = new SLocalRepo(srepo, fileCache);
		if (srepo.getCommitId() > 0) {
			// Repository has commits..
			// Get the Latest..
			LOG.info(String.format("Repo %s has commit; latest=%d (date=%s)", srepo.getId(),srepo.getCommitId(),
					PrintUtils.print(srepo.getCommitDate())));
			repoCheckout(lrepo, true);
		} else {
			LOG.info(String.format("Repo %s has no commits. Nothing to do", srepo.getId()));
		}

	}

	public static void repoCheckout(SLocalRepo lrepo, boolean fAllNew) throws IOException {
		// All New mean we cannot have any clash of relpaths in the directory.

		// tmp Dir wil
		
		Map<String,SolderEntry> mapEntriesNow = lrepo.createEntryMap();
		
		

	}

	public static void repCommit(SRepo srepo, File fileCache)
			throws IOException {

		// SVault svault;
		// public SVault(String id,String schemaName,int tenantId,int aoId) throws
		// IOException {
		String cgName = SolderMain.getSolderContainerGroupName();
		ContainerGroup cg = ContainerGroup.get(cgName);
		
		Objects.requireNonNull(cg, " cg " + cgName);
		

		SLocalRepo lRepo = new SLocalRepo(srepo,fileCache);
		CommitInfo commitInfo = new CommitInfo(lRepo);
		
		if (!commitInfo.fNewCommit) {
			LOG.info(String.format("repCommit found no new changes, Nothing to do"));
			return;
		}
		
		//We may want to check if anything change at all...
		CryptoScheme cs = CryptoScheme.getDefault();
		Map<String, String> mapCommitInfo = new HashMap<>();
		mapCommitInfo.put("commit", "some message");
		SCommit scommit = new SCommit(srepo, commitInfo.getCHash(),mapCommitInfo,commitInfo.commitId);
		
	 
		
		MessageDigest md = tlMessageDigest.get();
		for (SolderEntry se : commitInfo.listAdd) {
			// Create a new item...
			md.reset();
			String name = cs.getUUID();

			Map<String, String> mapInfo = new HashMap<>();
			mapInfo.put("path", se.getRelPath());
			mapInfo.put("pid", SessionManager.getPid());

			File fileRep = se.file;
			
			Validator.checkFile(fileRep, "path " + se.getRelPath());

			BlobFS blob = new BlobFS(name, SolderVaultFactory.TYPE, srepo.getId(), commitInfo.commitId, mapInfo, null);
			BlobFileTransact bft = cg.beginFileTransact(blob);
			boolean fError = true;
			FileOutputStream fos = null;
			InputStream is = null;
			try {
				is = new FileInputStream(fileRep);
				File fileOut = bft.getFile();
				fos = new FileOutputStream(fileOut);
				DigestOutputStream dos = new DigestOutputStream(fos, md);
				IOUtils.copy(is, dos);
				dos.close();

				byte[] digest = md.digest();
				String digestNew = PrintUtils.toHexString(digest);
				blob.getInfo().put("digest", digestNew);
				if (!CompareUtils.stringEquals(digestNew, se.digest)) {
					String stError = String.format("Write digest mismatch for %s. writeDigest=%s, prevCalc=%s",
							se.getRelPath(), digestNew, se.digest);
					LOG.info(stError);
					throw new SolderException(stError);
				}
				fError = false;
				bft.commit();
				se.setBlobId(blob.getId());

			} finally {
				IOUtils.closeQuietly(fos, is);
				if (fError) {
					bft.abort();
				}
			}
		}
		
		
		//Actual Commit file..
		//fileCommit
		Map<String, String> mapInfo = new HashMap<>();
		mapInfo.put("pid", SessionManager.getPid());
		String name = cs.getUUID();
		BlobFS blobCommit = new BlobFS(name, BLOB_TYPE_SOLDER_COMMIT, srepo.getId(), commitInfo.commitId, mapInfo, null);
		BlobFileTransact bft = cg.beginFileTransact(blobCommit);
		boolean fError = true;
		FileOutputStream fos = null;
		InputStream is = null;
		try {
			is = new FileInputStream(commitInfo.fileCommit);
			File fileOut = bft.getFile();
			fos = new FileOutputStream(fileOut);
			DigestOutputStream dos = new DigestOutputStream(fos, md);
			IOUtils.copy(is, dos);
			dos.close();
			fError = false;
			bft.commit();
			//Create SCommit.
			scommit.insert();
			srepo.updateCommit(scommit);

		} finally {
			IOUtils.closeQuietly(fos, is);
			if (fError) {
				bft.abort();
			}
		}
		
		//Do the actual deletions by changing ownere..
		// Use to delete
		
		if (commitInfo.listDel.size()>0) {
			List<BlobFS> listPrev = BlobFS.selectByOwner(SolderVaultFactory.TYPE, srepo.getId());
			LOG.info(String.format("SVault rep %s found %d objects.", srepo.getId(), listPrev.size()));

			Map<String, BlobFS> mapBlobFSPrev = new LinkedHashMap<>();
			for (BlobFS blobFS : listPrev) {
				String stRelPath = blobFS.getInfo().get("path");
				if (stRelPath != null) {
					mapBlobFSPrev.put(stRelPath, blobFS);
				}
			}
			String ownerDelete = srepo.getId() + "_del";
			for (SolderEntry se : commitInfo.listDel) {
				BlobFS fsToDelete = mapBlobFSPrev.get(se.getRelPath());
				if (fsToDelete != null) {
					fsToDelete.updateOwner(null, ownerDelete);
				}
			}
		}
		
		//Update your local REP....
		// BeechLCommit lcommit;
		
		lRepo.commitId= commitInfo.commitId;
		lRepo.chash = commitInfo.cHash;
		lRepo.mapEntry = commitInfo.makeLocalRepoMap();
		lRepo.commitLocalRepo(false);
		
	}

}
