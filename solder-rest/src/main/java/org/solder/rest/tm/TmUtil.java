package org.solder.rest.tm;

import java.io.IOException;
import java.util.Objects;

import org.solder.rest.client.TMRestOp;

import com.ee.rest.RestOp.RestClient;
import com.jnk.util.TReference;

public class TmUtil {
	
	
	public static String genTMChart(RestClient client) throws IOException {
		Objects.requireNonNull(client, "client");

		TReference<String> ret = new TReference<>();
		client.doRestCall(TMRestOp.TM_GEN_CHART, (_) -> {
			// You dont have to send this if it is false.

		}, (decoder) -> {
			ret.set(decoder.readString("ret"));
		});
		return ret.get();
	}

}
