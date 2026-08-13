package org.solder.rest.client;

import java.io.IOException;

import org.apache.commons.io.function.IOConsumer;
import org.apache.commons.io.function.IOFunction;

import com.ee.rest.RestOp;
import com.lnk.serializer.Encoder;

//Once we have more than one function, we can make create a tmrest project.
public enum TMRestOp implements RestOp {
		
	TM_GEN_CHART("tmgch",null,false,false);
	
	
	String op;
	boolean fRequestStream, fResponseStream;
	IOFunction<IOFunction<String, String>, IOConsumer<Encoder>> fnAutoBoxer;

	private TMRestOp(final String op) {
		this(op, null, false, false);
	}

	private TMRestOp(final String op, IOFunction<IOFunction<String, String>, IOConsumer<Encoder>> fnAutoBoxer,
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