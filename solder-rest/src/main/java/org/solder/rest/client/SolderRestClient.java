package org.solder.rest.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.function.IOConsumer;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.solder.rest.client.RemoteRepoSync.SolderEntry;

import com.ee.rest.RestException;
import com.ee.rest.RestOp.RestClient;
import com.ee.rest.client.EnigmaRestClient;
import com.jnk.util.PrintUtils;
import com.jnk.util.TReference;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;

public class SolderRestClient {
	
	
	public static SRepoInfo createRepo(String repoId,String schemaName,int aoId, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		TReference<SRepoInfo> ret = new TReference<>();
		client.doRestCall(SolderRestOp.CREATE, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
			encoder.writeString("tschema", schemaName);
			encoder.writeInt("ao_id", aoId);
		}, (decoder) -> {
			ret.set(EnigmaRestClient.setClient(decoder.readObject("ret", SRepoInfo.class), client));
		});
		return ret.get();
	}
	
	
	public static SRepoInfo getRepo(String repoId,RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		TReference<SRepoInfo> ret = new TReference<>();
		client.doRestCall(SolderRestOp.GET, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
		}, (decoder) -> {
			ret.set(EnigmaRestClient.setClient(decoder.readObject("ret", SRepoInfo.class), client));
		});
		return ret.get();
	}
	
	
	public static SCommitInfo  getLatestCommit(String repoId, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		TReference<SCommitInfo> ret = new TReference<>();
		client.doRestCall(SolderRestOp.GET_LATEST_COMMIT, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
		}, (decoder) -> {
			ret.set(EnigmaRestClient.setClient(decoder.readObject("ret", SCommitInfo.class), client));
		});
		return ret.get();
	}
	
	
	public static int  getNewCommitId(String repoId, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		MutableInt id = new MutableInt(-1);
		client.doRestCall(SolderRestOp.GEN_NEW_COMMIT_ID, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
		}, (decoder) -> {
			id.setValue(decoder.readInt("ret"));
		});
		return id.intValue();
	}
	
	public static void  downloadFile(String repoId,String relPath,long blobFsId,IOConsumer<SCommitInfo> cCommitInfo,IOSupplier<OutputStream> suppOs, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		client.doStreamRestCall(SolderRestOp.DOWNLOAD_FILE, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
			encoder.writeString("rel_path", relPath);
			encoder.writeLong("blob_fsid", blobFsId);
			
		},null, (decoder) -> {
			SCommitInfo commitInfo = EnigmaRestClient.setClient(decoder.readObject("ret", SCommitInfo.class), client);
			if (cCommitInfo != null) {
				cCommitInfo.accept(commitInfo);
			}
		},suppOs);
		
	}
	
	public static int uploadFile(String repoId,SolderEntry se,IOSupplier<InputStream> suppIs, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		MutableInt id = new MutableInt(-1);
		client.doStreamRestCall(SolderRestOp.UPLOAD_FILE, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
			encoder.writeObject("se", se,false);
		}, suppIs,(decoder) -> {
			id.setValue(decoder.readInt("ret"));
		},null);
		return id.intValue();
	}
	
	
	public static long commitUpload(String repoId,SCommitInfo sci,String digest,List<String> listDel,IOSupplier<InputStream> suppIs, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		Objects.requireNonNull(sci, "commitInfo");
		Validator.require(digest, "digest",Rules.NO_NULL_EMPTY);
		if (sci.getBlobFsId()>0) {
			throw new RestException("Illegal commitInfo; blobFSId is already set as "+sci.getBlobFsId());
		}
		
		MutableLong id = new MutableLong(-1);
		
		String[] aDel = listDel == null?null:listDel.toArray(PrintUtils.EMPTY_STRING_ARRAY);
		
		client.doStreamRestCall(SolderRestOp.COMMIT_UPLOAD, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
			encoder.writeObject("sci", sci,false);
			encoder.writeString("digest", digest);
			encoder.writeStringArray("del_rel_paths", aDel);
		},suppIs, (decoder) -> {
			id.setValue(decoder.readLong("ret"));
		},null);
		//We will let the Service object to set the fsId inside the commitInfo.
		return id.longValue();
	}
	
	
}
