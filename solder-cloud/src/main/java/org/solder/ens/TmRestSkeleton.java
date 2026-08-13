package org.solder.ens;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.function.IOConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.rest.client.TMRestOp;
import org.solder.telemetry.SolderTelemetryWriter;

import com.ee.ens.AbstractHttpServlet.SCall;
import com.ee.ens.EnServlet;
import com.ee.ens.EnigmaRestSkeleton;
import com.ee.rest.RestOp;
import com.ee.rest.RestProcessor;
import com.ee.rest.RestSkeletonState;
import com.ee.session.db.SentryProvider;
import com.jnk.util.TReference;
import com.lnk.lucene.RunOnce;

public enum TmRestSkeleton {
	
	TM_GEN_CHART(TMRestOp.TM_GEN_CHART,TmRestSkeleton::doTmGenChart);
	
	
	private static Log LOG = LogFactory.getLog(TmRestSkeleton.class.getName());
	
	RestOp restOp;
	IOConsumer<RestSkeletonState> cHandler;
	
	private TmRestSkeleton(RestOp restOp,IOConsumer<RestSkeletonState> cHandler) {
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
	
	
	static void doTmGenChart(RestSkeletonState state) throws IOException {
		SCall scall = (SCall)state.getCallObject();
		
		TReference<String> ref = new TReference<>();
		// We take string param val and optional param count
		// and return the same val as an array of count values.
		state.readParam((decoder) -> {
			// int count = decoder.readInt("count");
			scall.handleSession(decoder,null,false);
			EnigmaRestSkeleton.doSentryCheck(SentryProvider.NAMEDOP_ADMIN);
			File fileChart = SolderTelemetryWriter.generateCharts();
			ref.set(fileChart.getAbsolutePath());
		});

		// Return
		state.setSuccess((encoder) -> {
			encoder.writeString("ret", ref.get());
		});
	}

}
