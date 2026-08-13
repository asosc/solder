package org.solder.core;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.rest.solder.CommitSession;
import org.solder.rest.solder.IRepoFileService;
import org.solder.rest.solder.SCommitInfo;
import org.solder.rest.solder.SRepoInfo;
import org.solder.rest.solder.SolderEntry;

public class ServerRepoFileService implements IRepoFileService {

	private static Log LOG = LogFactory.getLog(ServerRepoFileService.class.getName());

	static final ServerRepoFileService INSTANCE = new ServerRepoFileService();

	public static ServerRepoFileService get() {
		return INSTANCE;
	}

	private ServerRepoFileService() {
	}

	public SRepoInfo getRepo(String repoId) throws IOException {
		SRepo repo = SRepo.getRepoById(repoId);
		Objects.requireNonNull(repo, () -> "repo " + repoId);
		return SRepo.makeSRepoInfo(repo);
	}

	public void refresh(SRepoInfo repoInfo) throws IOException {
		Objects.requireNonNull(repoInfo, "repo info");
		SRepoInfo repoInfoRefresh = getRepo(repoInfo.getId());
		repoInfo.refresh(repoInfoRefresh);
	}

	public SCommitInfo getLatestCommit(SRepoInfo srepoInfo) throws IOException {
		SRepo srepo = SRepo.getSRepo(srepoInfo);
		SCommit commit = srepo.getLatestCommit();
		return SCommit.makeSCommitInfo(commit);
	}

	public SCommitInfo getCommit(SRepoInfo srepoInfo, int commitId) throws IOException {
		SRepo srepo = SRepo.getSRepo(srepoInfo);
		SCommit commit = srepo.getCommit(commitId);
		return SCommit.makeSCommitInfo(commit);
	}

	public File downloadFile(SRepoInfo srepoInfo, String relPath, long blobFsId, String stDigestExpected)
			throws IOException {
		SRepo srepo = SRepo.getSRepo(srepoInfo);
		return srepo.downloadFile(relPath, blobFsId, stDigestExpected);
	}

	public SCommitInfo createSCommit(SRepoInfo srepoInfo, String chash, Map<String, String> mapInfo, int commitId)
			throws IOException {
		SRepo srepo = SRepo.getSRepo(srepoInfo);
		SCommit scommit = new SCommit(srepo, chash, mapInfo, commitId);
		return SCommit.makeSCommitInfo(scommit);
	}

	public CommitSession beginCommit(SCommitInfo commitInfoReq, List<String> listAddEntryRelPath,
			List<String> listDelEntryRelPath) throws IOException {

		SRepo repo = SRepo.getRepoBySeqId(commitInfoReq.getRepoSeqId());
		Objects.requireNonNull(repo, () -> "repo " + commitInfoReq.getRepoSeqId());

		// Same tip check as SSCommit / REST beginCommit.
		repo.requireExpectedTip(commitInfoReq.getPrevId());

		// Server uses this, so need for ecid;
		int commitId = SCommit.generateCommitId();
		String ecid = null;

		CommitSession cs = new CommitSession(commitId, ecid);
		cs.setParent(repo);
		cs.set(commitInfoReq, listAddEntryRelPath, listDelEntryRelPath);
		return cs;
	}

	public long uploadFile(CommitSession cs, SolderEntry se) throws IOException {
		SRepo repo = (SRepo) cs.getParent();
		Objects.requireNonNull(repo, "Repo not set!");
		return repo.uploadFile(se, se.getFile());
	}

	public SCommitInfo uploadCommit(CommitSession cs, File fileCommit) throws IOException {
		SRepo repo = (SRepo) cs.getParent();
		Objects.requireNonNull(repo, "Repo not set!");

		SCommitInfo commitInfoReq = cs.getCommitInfo();
		// Create a new
		SCommit scommit = repo.commitUpload(cs.getCommitId(), commitInfoReq, fileCommit);
		Objects.requireNonNull(scommit, "scommit after upload");
		return SCommit.makeSCommitInfo(scommit);
	}

}
