package org.solder.rest.solder;

import java.io.IOException;
import java.util.List;

import com.ee.rest.RestOp.RestClient;
import com.ee.rest.eobj.IEnigmaCloseable;
import com.lnk.lucene.record.RecordUtil;
import com.lnk.serializer.Decoder;
import com.lnk.serializer.Encoder;
import com.lnk.serializer.ISerializable;

public class CommitSession implements ISerializable,IEnigmaCloseable {
	
	int commitId;
	
	
	
	String ecid;
	RestClient rcForClose;
	
	
	transient List<String> listAdd, listDel;
	transient SCommitInfo commitInfo;
	transient Object parent;
	
	public CommitSession() {
		
	}
	
	public CommitSession(int commitId,String ecid) {
		this.commitId = commitId;
		this.ecid = ecid;
	}
	
	
	public void serialize(Encoder encoder) throws IOException {
		encoder.writeInt("commit_id", commitId);
		encoder.writeString("ecid", ecid);
	}

	public void deserialize(Decoder decoder) throws IOException {
		commitId = decoder.readInt("commit_id");
		ecid = decoder.readString("ecid");
	}
	
	public int getCommitId() {
		return commitId;
	}
	
	
	public void clearECId() {
		ecid = null;
	}
	
	public void setRcForClose(RestClient rcForClose) {
		this.rcForClose = rcForClose;
	}
	
	public RestClient getRcForClose() {
		return rcForClose;
	}

	

	public String toString() {
		return RecordUtil.printJson(this, false);
	}
	

	public String getECId() {
		return ecid;
	}
	
	//transient set..
	public void set( SCommitInfo commitInfo, List<String> listAdd, List<String> listDel ) {
		this.commitInfo = commitInfo;
		this.listAdd = listAdd;
		this.listDel = listDel;
	}
	
	public SCommitInfo getCommitInfo() {
		return commitInfo;
	}
	
	public List<String> getAddRelPaths() {
		return listAdd;
	}

	public List<String> getDelRelPaths() {
		return listDel;
	}
	
	public void setParent(Object obj) {
		this.parent = obj;
	}
	
	public Object getParent() {
		return parent;
	}
}
