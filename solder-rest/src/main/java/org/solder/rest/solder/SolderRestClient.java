package org.solder.rest.solder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.mutable.MutableLong;
import org.solder.rest.client.SolderRestOp;

import com.ee.rest.RestException;
import com.ee.rest.RestOp.RestClient;
import com.jnk.util.TReference;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;


public class SolderRestClient {
	
	//Solder is not expected to keep all versions for ever 
	//This is not a git, more for binary repositories and filesystem, logfiles, etc.
	
	
	public static SRepoInfo createRepo(String repoId,String schemaName,int aoId,String tag, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		TReference<SRepoInfo> ret = new TReference<>();
		client.doRestCall(SolderRestOp.CREATE, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
			encoder.writeString("tschema", schemaName);
			encoder.writeInt("ao_id", aoId);
			encoder.writeString("tag", tag);
		}, (decoder) -> {
			ret.set(decoder.readObject("ret", SRepoInfo.class));
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
			ret.set(decoder.readObject("ret", SRepoInfo.class));
		});
		return ret.get();
	}
	
	
	public static SRepoInfo[] searchRepo(String repoIdWild,String schemaWild,String tagFilter,RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");

		TReference<SRepoInfo[]> ret = new TReference<>();
		client.doRestCall(SolderRestOp.SEARCH, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("idWild", repoIdWild);
			encoder.writeString("tschemaWild", schemaWild);
			//Exact match or Null. (case sensitive) 
			encoder.writeString("tagFilter", tagFilter);
		}, (decoder) -> {
			ret.set(decoder.readObjectArray("ret", SRepoInfo.class));
		});
		return ret.get();
	}
	
	public static SRepoInfo updateRepo(String repoId,String tag,RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		
		String repoIdFinal = Validator.require(repoId, "repo id", Rules.TRIM_LOWER,Rules.NO_NULL_EMPTY);
		Objects.requireNonNull(tag,"tag cannot be null! send empty instead!");

		
		TReference<SRepoInfo> ret = new TReference<>();
		client.doRestCall(SolderRestOp.UPDATE, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoIdFinal);
			encoder.writeString("tag", tag);
		}, (decoder) -> {
			ret.set(decoder.readObject("ret", SRepoInfo.class));
		});
		return ret.get();
	}
	
	
	public static SRepoInfo deleteRepo(String repoId,RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		
		String repoIdFinal = Validator.require(repoId, "repo id", Rules.TRIM_LOWER,Rules.NO_NULL_EMPTY);
		
		
		TReference<SRepoInfo> ret = new TReference<>();
		client.doRestCall(SolderRestOp.DELETE, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoIdFinal);
		}, (decoder) -> {
			ret.set(decoder.readObject("ret", SRepoInfo.class));
		});
		return ret.get();
	}
	
	public static SCommitInfo  getLatestCommit(String repoId, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		TReference<SCommitInfo> ret = new TReference<>();
		client.doRestCall(SolderRestOp.GET_LATEST_COMMIT, (encoder) -> {
			
			encoder.writeString("id", repoId);
		}, (decoder) -> {
			ret.set(decoder.readObject("ret", SCommitInfo.class));
		});
		return ret.get();
	}
	
	//If you ask for specific commit Id and it is valid, it will throw an error.
	public static SCommitInfo[]  getCommits(String repoId,int[] aCommitIds, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		TReference<SCommitInfo[]> ret = new TReference<>();
		client.doRestCall(SolderRestOp.GET_COMMIT, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
			encoder.writeIntArray("commits", aCommitIds);
		}, (decoder) -> {
			ret.set(decoder.readObjectArray("ret", SCommitInfo.class));
		});
		return ret.get();
	}
	
	public static SCommitInfo[]  getCommits(String repoId,RestClient client) throws IOException {
		return getCommits(repoId,null,client);
	}
	
	public static SCommitInfo  getCommit(String repoId, int commitId,RestClient client) throws IOException {
		 SCommitInfo[] a = getCommits(repoId,new int[] {commitId},client);
		 if (a!=null) {
			 for (SCommitInfo sci : a) {
				 if (sci.getId() == commitId) {
					 return sci;
				 }
			 }
		 }
		throw new RestException("Unknown commitId "+commitId+" for repo "+repoId);
	}
	
	
	
	
	

	
	
	public static void  downloadFile(String repoId,String relPath,long blobFsId,String digestExpected,IOSupplier<OutputStream> suppOs, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		client.doStreamRestCall(SolderRestOp.DOWNLOAD_FILE, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
			encoder.writeString("rel_path", relPath);
			encoder.writeLong("blob_fsid", blobFsId);
			encoder.writeString("digest_expect", digestExpected);
			
		},null, (_) -> {
			//Digest check can be done by suppOs in addition to expectation verification.
			//This way client can use different types of digests. (Server currently uses SHA-256)
			//CRC32 automatically done for transport.
			
		},suppOs);
		
	}
	
	public static CommitSession beginCommit(SCommitInfo commitInfoReq,String[] aStRelPathAdd,String[] aStRelPathDel, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		Objects.requireNonNull(commitInfoReq,"Commit Request");
		//Add contains both updated and new files. (Update can be found using the previous commit, if needed)
		//Del only contain removed files
		
		TReference<CommitSession> ref = new TReference<>();
		
		client.doRestCall(SolderRestOp.BEGIN_COMMIT, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeInt("sid", commitInfoReq.getRepoSeqId());
			encoder.writeObject("commit_req", commitInfoReq,false);
			encoder.writeStringArray("rpath_add", aStRelPathAdd);
			encoder.writeStringArray("rpath_del", aStRelPathDel);
		}, (decoder) -> {
			ref.set(decoder.readObject("ret", CommitSession.class));
		});
		return ref.get();
	}
	
	
	public static long uploadFile(CommitSession cs,SolderEntry se,IOSupplier<InputStream> suppIs, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		Objects.requireNonNull(cs, "Commit Session");
		
		MutableLong id = new MutableLong(-1);
		client.doStreamRestCall(SolderRestOp.UPLOAD_FILE, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("ecid", cs.getECId());
			encoder.writeObject("se", se,false);
		}, suppIs,(decoder) -> {
			id.setValue(decoder.readLong("ret"));
		},null);
		return id.longValue();
	}
	
	
	public static SCommitInfo uploadCommit(CommitSession cs,File fileCommit,String digest, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		Objects.requireNonNull(cs,"commitSession");
		Validator.checkFile(fileCommit, "Commit File");
		Validator.require(digest, "digest",Rules.NO_NULL_EMPTY);
		
		
		TReference<SCommitInfo> ref = new TReference<>();
		
		
		IOSupplier<InputStream> suppIs = ()->{
			return new FileInputStream(fileCommit);
		};
		
		
		client.doStreamRestCall(SolderRestOp.UPLOAD_COMMIT, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("ecid", cs.getECId());
			encoder.writeString("digest", digest);
		},suppIs, (decoder) -> {
			ref.set(decoder.readObject("ret",SCommitInfo.class));
		},null);
		//We will let the Service object to set the fsId inside the commitInfo.
		return Objects.requireNonNull(ref.get());
	}
	
	
}
