package org.solder.rest.client;

import java.io.IOException;

import org.apache.commons.io.function.IOConsumer;
import org.apache.commons.io.function.IOFunction;

import com.ee.rest.RestOp;
import com.lnk.serializer.Encoder;

public enum SolderRestOp implements RestOp {

		CREATE("solcreate", SolderRestOp::autoboxSolder, false, false),
		GET("solget", SolderRestOp::autoboxSolder, false, false),
		GET_LATEST_COMMIT("solglc", SolderRestOp::autoboxSolder, false, false),
		DOWNLOAD_FILE("soldf", SolderRestOp::autoboxSolder, false, true),
		GEN_NEW_COMMIT_ID("solgnci", SolderRestOp::autoboxSolder, false, false),
		
		UPLOAD_FILE("soluf", null, true, false),
		COMMIT_UPLOAD("solcu", null, true, false);
		
	
	// AutoBoxers...
		public static IOConsumer<Encoder> autoboxSolder(IOFunction<String, String> fnValue) throws IOException {
			return (encoder) -> {
					RestOp.addIfAvailable(encoder,fnValue,Session_KEY,"repo_id","schema","ao_id","rel_path","blob_fsid");
			};
		}
	
	String op;
	boolean fRequestStream, fResponseStream;
	IOFunction<IOFunction<String, String>, IOConsumer<Encoder>> fnAutoBoxer;

	private SolderRestOp(final String op) {
		this(op, null, false, false);
	}

	private SolderRestOp(final String op, IOFunction<IOFunction<String, String>, IOConsumer<Encoder>> fnAutoBoxer,
			boolean fRequestStream, boolean fResponseStream) {
		this.op = op;
		this.fnAutoBoxer = fnAutoBoxer;
		this.fRequestStream = fRequestStream;
		this.fResponseStream = fResponseStream;

		RestOp.register(this);
	}
	
	
	
	

	public String getOp() {
		return op;
	}
	
	public IOConsumer<Encoder> autoBoxJson(IOFunction<String, String> fnValue) throws IOException {
		return fnAutoBoxer.apply(fnValue);
	}

	public boolean hasRequestStream() {
		return fRequestStream;
	}

	public boolean hasResponseStream() {
		return fResponseStream;
	}

	public boolean requireSession() {
		return true;
	}
		
		
		
		
		
		
		
		
	
}