package org.solder.rest.skel;



import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.function.IOConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.rest.client.SolderRestOp;
import org.solder.vsync.SolderVaultFactory;
import org.solder.vsync.SolderVaultFactory.SRepo;

import com.ee.ens.EnServlet;
import com.ee.ens.EnServlet.SCall;
import com.ee.rest.RestException;
import com.ee.rest.RestOp;
import com.ee.rest.RestProcessor;
import com.ee.rest.RestSkeletonState;
import com.ee.session.SessionManager;
import com.ee.session.db.EEvent;
import com.ee.session.db.Event;
import com.ee.session.db.User;
import com.jnk.util.TReference;
import com.jnk.util.Validator;
import com.jnk.util.Validator.Rules;
import com.lnk.lucene.RunOnce;


public enum SolderRestSkeleton {

	CREATE(SolderRestOp.CREATE,SolderRestSkeleton::doCreate);
		
	private static Log LOG = LogFactory.getLog(SolderRestSkeleton.class.getName());
	
	RestOp restOp;
	IOConsumer<RestSkeletonState> cHandler;
	
	private SolderRestSkeleton(RestOp restOp,IOConsumer<RestSkeletonState> cHandler) {
		this.restOp = restOp;
		this.cHandler = cHandler;
	}
	
	
	static final AtomicBoolean s_fInit = new AtomicBoolean(false);
	static Map<String,String> s_mapContentType;
	
	 
	

	public static void init() throws IOException {
		LOG.info("SolderRestSkeleton Init called.. isServerInit="+EnServlet.isEnServletInitCalled());
		RunOnce.ensure(s_fInit, () -> {
			
			if (EnServlet.isEnServletInitCalled()) {
				SolderRestSkeleton[] a = SolderRestSkeleton.class.getEnumConstants();
				for (SolderRestSkeleton skel : a) {
					RestProcessor.register(skel.restOp.getOp(), skel.cHandler);
				}
			} else {
				LOG.info("SolderRestSkeleton Init called, No servlet found, Not doing anything..");
			}
		});
		
		

	}
	
	static void ensureTenant(int resourceTenantId,int userTenantId, String objId) throws IOException {
		if (resourceTenantId!=userTenantId ) {
			Event.log(EEvent.Security_Warning, resourceTenantId, userTenantId, (mb) -> {
				mb.put("obj_id", objId);
			});
			throw new RestException("Invalid object;");
		}
	}
	
	
	// Skeletons
	static void doCreate(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		TReference<SRepo> refA = new TReference<>();
		// We take string param val and optional param count
		// and return the same val as an array of count values.
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			User user = (User)SessionManager.getUser();
			
			Set<String> params =decoder.getAllObjectFields();
			String repoId = decoder.readString("id");
			repoId = Validator.require(repoId, "repo id", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			String schema = decoder.readString("tschema");
			schema = Validator.require(schema, "schema", Rules.NO_NULL_EMPTY,Rules.TRIM_LOWER);
			int aoId = params.contains("ao_id")?decoder.readInt("ao_id"):0;
			
			SRepo repo = SolderVaultFactory.ensureSRepo(repoId, schema, user.getTenantId(), aoId);
			
						
			Objects.requireNonNull(repo,"repo");
			ensureTenant(repo.getTenantId(),user.getTenantId(),repo.getId());
			refA.set(repo);
		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeObject("ret", refA.get(),false);
		});
		
		
	}
	
}