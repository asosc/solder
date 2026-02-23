package org.solder.rest.client;

import java.io.IOException;
import java.util.Objects;

import org.solder.rest.client.SolderRestObjects.RSRepo;

import com.ee.rest.RestOp.RestClient;
import com.ee.rest.client.EnigmaRestClient;
import com.jnk.util.TReference;

public class SolderRestClient {
	
	
	public static RSRepo createRepo(String repoId,String schemaName,int aoId, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		TReference<RSRepo> ret = new TReference<>();
		client.doRestCall(SolderRestOp.CREATE, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
			encoder.writeString("tschema", schemaName);
			encoder.writeInt("ao_id", aoId);
		}, (decoder) -> {
			ret.set(EnigmaRestClient.setClient(decoder.readObject("ret", RSRepo.class), client));
		});
		return ret.get();
	}
	
	
	public static RSRepo getRepo(String repoId,String schemaName,int aoId, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		TReference<RSRepo> ret = new TReference<>();
		client.doRestCall(SolderRestOp.CREATE, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
			encoder.writeString("tschema", schemaName);
			encoder.writeInt("ao_id", aoId);
		}, (decoder) -> {
			ret.set(EnigmaRestClient.setClient(decoder.readObject("ret", RSRepo.class), client));
		});
		return ret.get();
	}
	
	
	public static RSRepo createRepo2(String repoId,String schemaName,int aoId, RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");
		
		TReference<RSRepo> ret = new TReference<>();
		client.doRestCall(SolderRestOp.CREATE, (encoder) -> {
			// You dont have to send this if it is false.
			encoder.writeString("id", repoId);
			encoder.writeString("tschema", schemaName);
			encoder.writeInt("ao_id", aoId);
		}, (decoder) -> {
			ret.set(EnigmaRestClient.setClient(decoder.readObject("ret", RSRepo.class), client));
		});
		return ret.get();
	}
	
	/*
	 * mapGitOpsHelp.put("create",
	 * "Git create. Params: fileLocalRepo repoId schemaName [tenant_id aoId]");
	 * mapGitOpsHelp.put("checkout", "Git Checkout(same as clone,rebase). Params:");
	 * mapGitOpsHelp.put("push", "Git Push(same as commit and push). Params:");
	 * mapGitOpsHelp.put("init", "Git init. Params:repoId");
	 */

}
