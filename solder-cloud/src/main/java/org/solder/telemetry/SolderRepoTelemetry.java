package org.solder.telemetry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.solder.core.SRepo;
import org.solder.core.SRepoUtil;

import com.ee.session.SessionManager;
import com.lnk.lucene.FileNameUtil;

public class SolderRepoTelemetry {
	
	
	public static File generateRepoUsage(int[] aRepoSid,boolean fDeletedRepoOnly) throws IOException {
		
		if (aRepoSid!=null && aRepoSid.length==0) {
			aRepoSid = null;
		} 
		List<SRepo> list = new ArrayList<>();
		if (aRepoSid != null && aRepoSid.length==1) {
			int id = aRepoSid[0];
			SRepo srepo = SRepo.getRepoBySeqId(id);
			Objects.requireNonNull(srepo,()->"Repo sid "+id);
			list.add(srepo);
		} else {
			List<SRepo> listToCheck = fDeletedRepoOnly?SRepo.getDeletedRepo():SRepo.getAll();
			if (aRepoSid!=null) {
				int[] idFinal = aRepoSid;
				Set<Integer> set = Arrays.stream(idFinal).boxed().collect(Collectors.toSet());
				listToCheck.stream().filter((s)->set.contains(s.getSeqId())).forEach(list::add);
			}
		}
		
		File fileLogRoot = SessionManager.getLogRoot();
		File fileRepoUsageRoot = new File(fileLogRoot,"SolderRepoUsage");
		File fileRepoUsage = new File(fileRepoUsageRoot,FileNameUtil.generateGMT("SRepo"));
		
		SRepoUtil.getAllRepoUsage(list, fileRepoUsage);
		return fileRepoUsage;
		
	}
	

}
