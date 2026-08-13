package org.solder.ens;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.core.SCommit;
import org.solder.core.SRepo;
import org.solder.rest.solder.CommitDetails;
import org.solder.rest.solder.CommitSession;
import org.solder.rest.solder.SCommitInfo;
import org.solder.rest.solder.SolderEntry;

import com.ee.session.db.EStateObj;

public class SSCommit  implements Closeable{
	
	
	static Log LOG = LogFactory.getLog(SSCommit.class.getName());
	
	
	
	SRepo srepo;
	String[] aStRelPathMod;
	String[] aStRelPathDel;
	
	int commitId;
	
	CommitSession cs;
	
	String ecid;
	boolean fClosing = false;
	
	public SSCommit(SRepo srepo,SCommitInfo sci,String[] aStRelPathMod,String[] aStRelPathDel) throws IOException {
		this.srepo= Objects.requireNonNull(srepo,"srepo");
		this.aStRelPathMod = aStRelPathMod;
		this.aStRelPathDel = aStRelPathDel;
		
		ecid = EStateObj.generateECId();
		//Server uses this, so need for ecid;
		commitId = SCommit.generateCommitId();
	
		cs = new CommitSession(commitId,ecid);
		cs.set(sci, CommitDetails.toList(aStRelPathMod),  CommitDetails.toList(aStRelPathDel));
		EStateObj.cache(ecid,this,(ssc)-> IOUtils.closeQuietly(ssc));
	}
	
	
	public CommitSession getCommitSession() {
		return cs;
	}
	
	public long upload(SolderEntry se,File fileContent) throws IOException {
		return srepo.uploadFile(se,fileContent);
	}
	
	public SCommit uploadCommit(File fileCommit) throws IOException {
		SCommitInfo commitInfoReq = cs.getCommitInfo();
		//Create a new 
		return srepo.commitUpload(cs.getCommitId(),commitInfoReq, fileCommit);
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
