package org.solder.telemetry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.solder.core.SRepo;
import org.solder.core.SRepoUtil;
import org.solder.core.SolderException;

import com.ee.session.SessionManager;
import com.lnk.lucene.FileNameUtil;

public class SolderRepoTelemetry {
	
	
	public static File generateRepoUsage(int[] aRepoSid,boolean fDeletedRepoOnly) throws IOException {
		
		if (aRepoSid!=null && aRepoSid.length==0) {
			aRepoSid = null;
		}

		List<SRepo> listToCheck = fDeletedRepoOnly ? SRepo.getDeletedRepo() : SRepo.getAll();
		List<SRepo> list;
		if (aRepoSid == null) {
			list = listToCheck;
		} else {
			Set<Integer> setWanted = Arrays.stream(aRepoSid).boxed()
					.collect(Collectors.toCollection(LinkedHashSet::new));
			list = new ArrayList<>();
			Set<Integer> setFound = new HashSet<>();
			for (SRepo srepo : listToCheck) {
				if (setWanted.contains(srepo.getSeqId())) {
					list.add(srepo);
					setFound.add(srepo.getSeqId());
				}
			}
			for (int sid : setWanted) {
				if (!setFound.contains(sid)) {
					throw new SolderException(String.format("Repo sid %d not found%s", sid,
							fDeletedRepoOnly ? " among deleted repos" : ""));
				}
			}
		}
		
		File fileLogRoot = SessionManager.getLogRoot();
		File fileRepoUsageRoot = new File(fileLogRoot,"SolderRepoUsage");
		File fileRepoUsage = new File(fileRepoUsageRoot,FileNameUtil.generateGMT("SRepo"));
		if (!fileRepoUsage.mkdirs() && !fileRepoUsage.isDirectory()) {
			throw new IOException("Unable to create repo usage dir " + fileRepoUsage.getAbsolutePath());
		}
		
		SRepoUtil.getAllRepoUsage(list, fileRepoUsage);
		return fileRepoUsage;
		
	}
	

}
