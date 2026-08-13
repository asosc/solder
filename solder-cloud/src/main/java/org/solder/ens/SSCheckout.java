package org.solder.ens;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.core.SCommit;
import org.solder.core.SRepo;
import org.solder.core.SolderException;

import com.ee.session.db.EEvent;
import com.ee.session.db.EStateObj;
import com.ee.session.db.Event;

public class SSCheckout implements Closeable{
	
	
	static Log LOG = LogFactory.getLog(SSCommit.class.getName());
	
	String ecid;
	
	SRepo srepo;
	SCommit scommit;
	
	boolean fClosing = false;
	
	public SSCheckout(SRepo srepo,int commitId) throws IOException {
		this.srepo= Objects.requireNonNull(srepo,"srepo");
		
		
		
		if (commitId<=0) {
			scommit = srepo.getLatestCommit();
		} else {
			scommit = SCommit.selectCommitById(commitId);
			Objects.requireNonNull(scommit,"Commit id "+commitId);
			if (scommit.getRepoSeqId() != srepo.getSeqId()) {
				
				Event.log(EEvent.Security_Warning, srepo.getSeqId(),srepo.getTenantId(), (mb) -> {
					mb.put("fn", "SSCheckout_commit");
					mb.put("commitId", commitId);
					mb.put("commitRepoId", scommit.getRepoSeqId());
					mb.put("repoId",srepo.getId());
					mb.put("repoSid",srepo.getSeqId());
				});
				
				throw new SolderException("Invalid commitId "+commitId+"; not in repo");
			}
		}
		
		ecid = EStateObj.generateECId();
		EStateObj.cache(ecid,this,(ssc)-> IOUtils.closeQuietly(ssc));
	}
	
	
	
	
	public synchronized void close() throws IOException {
		if (fClosing) {
			return ;
		}
		fClosing = true;
		if (this.ecid!=null) {
			//Remove from cache
			EStateObj.remove(this.ecid);
			this.ecid=null;
		}
		
		
	}

}
