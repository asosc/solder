package org.solder.rest.solder;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.rest.solder.SolderEntry.EntryType;

import com.beech.bfs.BeechException;
import com.beech.bfs.BeechFS;
import com.beech.bfs.BeechLCommit;
import com.beech.bfs.BeechLDirectory;
import com.beech.bfs.Mode;
import com.ee.rest.RestException;
import com.jnk.util.CompareUtils;
import com.jnk.util.RelPath;
import com.jnk.util.TFileUtil;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.lnk.lucene.io.LDirectory;
import com.lnk.lucene.util.LogJsonDecoder;
import com.lnk.lucene.util.LogJsonEncoder;
import com.lnk.serializer.JsonDecoder;
import com.lnk.serializer.JsonEncoder;

public class SLocalRepo {
	
	private static Log LOG = LogFactory.getLog(SLocalRepo.class.getName());
	
	public static final String SOLDER_LOCAL_DIR = ".solder";
	public static final String SOLDER_LOCAL_REPO = "slrepo";
	public static final String LDIR_ROOT = "sl";
	public static final int LDIR_VERSION_1 = 1;
	public static final int LDIR_VERSION = 2;

	

	static String getLocalRepoCommitPath() {
		// .bee is intentionally taken out.
		return String.format("%s/%s", SOLDER_LOCAL_DIR, SOLDER_LOCAL_REPO);
	}

	static LDirectory getLDirectory(BeechFS fs, String dirRoot, Mode mode, IOConsumer<BeechFS> onClose)
			throws IOException {

		return new BeechLDirectory(fs, fs.getFileName(), dirRoot, mode, onClose);
	}
	
	
	
	public static void checkLocalRepoExists(File fileLocalRepoRoot) throws IOException {
		File fileCommitPath = new File(fileLocalRepoRoot, getLocalRepoCommitPath());
		if (!fileCommitPath.exists()) {
			throw new RestException("No local repo at "+fileLocalRepoRoot);
		}
	}
	
	public static String readLocalRepoId(File fileLocalRepoRoot) throws IOException {
		File fileCommitPath = new File(fileLocalRepoRoot, getLocalRepoCommitPath());
		if (!fileCommitPath.exists()) {
			throw new RestException("No local repo at "+fileLocalRepoRoot);
		}
		SLocalRepo lrepo = new SLocalRepo(fileLocalRepoRoot);
		return lrepo.repoId;
	}

	// BeechLCommit lcommit;
	String repoId;
	int commitId;
	String chash;

	File fileCommitLocalRepo;
	File fileDotSolder;
	Mode mode;
	BeechFS fs;
	LDirectory ldir;

	SRepoInfo srepo;
	File fileRoot;
	RelPath relPath;

	Map<String, SolderEntry> mapEntry;
	String stCommitDirRelPath;

	boolean fDirty = false;

	BeechLCommit lcommit;

	LDirectory getLDirectory() throws IOException {
		if (ldir == null) {

			if (fs == null) {
				fs = new BeechFS(fileCommitLocalRepo, mode);
			}

			IOConsumer<BeechFS> onClose = (fsClose) -> {
				IOUtils.closeQuietly(fsClose);
				this.fs = null;
			};
			ldir = new BeechLDirectory(fs, fs.getFileName(), LDIR_ROOT, mode, onClose);

		}
		Objects.requireNonNull(ldir, "ldir");
		return ldir;
	}
	
	private SLocalRepo(File fileLocalRepoRoot) throws IOException {
		checkLocalRepoExists(fileLocalRepoRoot);
		fileCommitLocalRepo = new File(fileLocalRepoRoot, getLocalRepoCommitPath());
		mode = Mode.READONLY;
		this.loadLocalRepo();
	}
	
	//TODO : Add .gitignore type file in the repo

	public SLocalRepo(SRepoInfo repo, File fileLocalRepoRoot,boolean fCreateLocalRepo) throws IOException {
		
		if (!fCreateLocalRepo) {
			checkLocalRepoExists(fileLocalRepoRoot);
		}

		srepo = Objects.requireNonNull(repo, "repo");
		Validator.checkDir(fileLocalRepoRoot, false, "repo dir");
		this.fileRoot = fileLocalRepoRoot;
		relPath = new RelPath(this.fileRoot);
		stCommitDirRelPath = srepo.getCommitDir();
		if (stCommitDirRelPath == null) {
			stCommitDirRelPath = "";
		}
		this.stCommitDirRelPath = stCommitDirRelPath.trim();

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
			Validator.checkDir(fileCommitLocalRepo.getParentFile(), true, "local repo");
			commitLocalRepo(true);
		}
	}

	public static final SolderEntry[] EMPTY_ARRAY = new SolderEntry[0];
	
	public String getRepoId() {
		return repoId;
	}
	
	public int getCommitId() {
		return commitId;
	}
	
	public String getCommitHash() {
		return chash;
	}
	
	public SRepoInfo getRepoInfo() {
		return srepo;
	}
	

	public Map<String, SolderEntry> getEntryMap() {
		return mapEntry;
	}
	

	void commitLocalRepo(boolean fCreate) throws IOException {
		Mode.checkWrite(mode, SOLDER_LOCAL_REPO);

		if (lcommit == null) {
			if (fCreate) {
				lcommit = new BeechLCommit(SOLDER_LOCAL_REPO);
			} else {
				throw new RestException("Unexpected Null lcommit");
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
				out.writeInt(commitId);
				out.writeString(chash);
				String stEntries = LogJsonEncoder.getTL().serialize((je)->{
					JsonEncoder.serialize(je, () -> {
						je.writeObjectArray("entries", aSolderEntry, false);
					});
				});
				out.writeString(stEntries);
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
		AtomicBoolean fUpgrade = new AtomicBoolean();
		try {
			if (ldir == null) {
				ldir = getLDirectory();
			}
			
			
			
			//AutoUpdate LCOmmit.
			lcommitNew.load(ldir, (in) -> {
				int version = in.readVInt();
				if (version != LDIR_VERSION && version != LDIR_VERSION_1) {
					throw new BeechException("Unknown version " + version + "; expect=" + LDIR_VERSION);
				}
				fUpgrade.set(version!=LDIR_VERSION);
				String repoIdRead = in.readString();
				if (repoId!=null) {
					if (!CompareUtils.stringEquals(repoId, repoIdRead)) {
						throw new BeechException("Name mismatch; repoIdRead=" + repoIdRead + "; expect " + repoId);
					}
				}else {
					repoId = repoIdRead;
				}

				stCommitDirRelPath = in.readString();
				if (version==LDIR_VERSION_1) {
					//Ignore removed field setExtensionsToAllow
					in.readSetOfStrings();
				}
				commitId = in.readInt();
				chash = in.readString();
				
				String stEntries = in.readString();
				LogJsonDecoder.getTL().readJson(stEntries, (jd) -> {
					JsonDecoder.deserialize(jd, () -> {
						SolderEntry[] aSolderEntry = jd.readObjectArray("entries", SolderEntry.class);
						mapEntry = new LinkedHashMap<>();
						for (SolderEntry entry : aSolderEntry) {
							mapEntry.put(entry.getRelPath(), entry);
						}
					});
				});

			});

		} finally {
			IOUtils.closeQuietly(ldir);
			ldir = null;
		}
		this.lcommit = lcommitNew;
		fDirty = false;
		if (fUpgrade.get()) {
			//Write out again..
			fDirty =true;
			mode=Mode.WRITE;
			commitLocalRepo(false);
		}

		

		// Spliterators.spliterator(aSegInfo,Spliterator.SIZED).
		LOG.debug(String.format("LocalRepo(%s)-> nEntry=%d; entryRelPath={%s}", repoId, mapEntry.size(),
				StringUtils.join(mapEntry.keySet(), ',')));
	}

	
	public Collection<File> scan() throws IOException{
		// Put directory Filter...
		
		FileFilter ignoreFile = TFileUtil.createIgnoreFileFilter(fileRoot, ".solderignore");
		FileFilter dirFilter = f -> TFileUtil.ignoreDirectoryNames(SOLDER_LOCAL_DIR).accept(f) && ignoreFile.accept(f);
		FileFilter fileFilter = ignoreFile; // or and with another file filter
		return TFileUtil.getAllFiles(fileRoot, fileFilter, dirFilter, true);
	}

	public Map<String, SolderEntry> createEntryMap(Map<String, SolderEntry> mapDotSolder) throws IOException {
		// Prior map may be empty but must be non-null (git-like: reuse digest when size+mtime match).
		Objects.requireNonNull(mapDotSolder,"map dot solder");
		Map<String, SolderEntry> mapEntriesNow = new TreeMap<>();
		Collection<File> collFile = scan();

		String prefix = null;
		if (stCommitDirRelPath != null && stCommitDirRelPath.length() > 0) {
			prefix = stCommitDirRelPath + "/";
		}

		for (File file : collFile) {
			String path = relPath.relativize(file.getAbsolutePath());
			EntryType etype = (prefix == null || path.startsWith(prefix)) ? EntryType.COMMIT : EntryType.BLOB;
			
			SolderEntry sePrev = mapDotSolder.get(path);
			SolderEntry sePrev2 = sePrev;
			if (sePrev !=null) {
				// Git-like racy-git avoidance lite: trust prior digest only when size+mtime+type match.
				boolean fDiff = sePrev.size != file.length() || sePrev.tModified != file.lastModified() || etype != sePrev.etype;
				if (fDiff) {
					LOG.info(String.format(
							"Rejecting previous entry; relpath=%s (size=%d,tModified=%d,type=%s) prev=(size=%d,tModified=%d,type=%s)",
							path, file.length(), file.lastModified(), etype.name(), sePrev.size, sePrev.tModified,
							sePrev.etype.name()));
					// Attributes changed: must recompute full digest (do not reuse sePrev.digest).
					sePrev = null;
				} 
			}
			
			SolderEntry se = new SolderEntry(path, etype, file, -1L, 0,sePrev);
			// Size+type unchanged and content digest matches prior: only mtime drifted
			// (common after checkout wrote content without restoring commit mtime).
			// Restore mtime so the next scan can skip the full digest.
			if (sePrev == null && sePrev2 != null && etype == sePrev2.etype && se.size == sePrev2.size
					&& CompareUtils.stringEquals(se.digest, sePrev2.digest)) {
				LOG.info(String.format("Repair %s modified date (want=%d, was=%d)", path, sePrev2.tModified,
						file.lastModified()));
				if (!file.setLastModified(sePrev2.tModified)) {
					LOG.warn(String.format("setLastModified failed for %s", file.getAbsolutePath()));
				}
				// Use actual FS mtime (may be rounded); avoid recreate/verifyPrev mismatch.
				se.tModified = file.lastModified();
			}
			mapEntriesNow.put(path, se);
			LOG.info(String.format("Collecting file %s", "" + se));
		}
		return mapEntriesNow;
	}

}
