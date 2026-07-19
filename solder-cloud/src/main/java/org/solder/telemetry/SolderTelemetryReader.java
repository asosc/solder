package org.solder.telemetry;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.core.SolderException;
import org.solder.vsync.SolderVaultFactory;
import org.solder.vsync.SolderVaultFactory.SRepo;

import com.beech.bfs.Mode;
import com.beech.compress.IContext;
import com.beech.store.TRecordDecoder;
import com.beech.store.TSMap;
import com.beech.store.TSegment;
import com.beech.store.TSegmentInfo;
import com.beech.store.TVault;
import com.ee.session.db.Machine;
import com.ee.session.db.TelemetrySchema;
import com.ee.session.db.Tenant;
import com.jnk.util.Validator;
import com.lnk.hdr.HdrHistogramSnap;
import com.lnk.lucene.FileNameUtil;

public class SolderTelemetryReader implements Closeable{

	private static Log LOG = LogFactory.getLog(SolderTelemetryReader.class.getName());

	public static SolderTelemetryReader localReader() throws IOException {
		// String.format("%04dq%d", iYear, iQuarter);

		LOG.info(String.format("loadLocalReader"));

		Machine machine = Machine.getThis();
		int aoId = machine.getId();
		String dateGroup = SolderTelemetryWriter.getDateGroupId();
		String schemaName = String.format("%s%s", SolderTelemetryWriter.SCHEMA_NAME_PREFIX, dateGroup);

		SRepo repo = SolderVaultFactory.getRepoByUnique(Tenant.ROOT_ID, schemaName, aoId);
		if (repo == null) {
			throw new SolderException(String.format("No repo found for %s (aoId=%d)", schemaName, aoId));
		}
		return new SolderTelemetryReader(repo);
	}

	TVault tvault;
	TSMap tsmEvent, tsmHdrHist;
	SRepo repo;
	boolean fClosed;

	SolderTelemetryReader(SRepo repo) throws IOException {
		// use the pid for now..
		this.repo = Objects.requireNonNull(repo);

		LOG.trace(String.format("GitSync %s loaded", repo.getId()));
		tvault = TVault.open(SolderVaultFactory.TYPE, repo.getId(), Mode.READONLY, TelemetrySchema.INSTANCE);

	}

	void checkOpen() throws IOException {
		if (fClosed) {
			throw new SolderException("SolderTelemetryRdader already closed");
		}
	}

	public void generateCharts(Date dateStart,Date dateEnd,File fileOut) throws IOException{
		checkOpen();
		File fileCharts = new File(fileOut,FileNameUtil.generate("HdrHist"));
		Validator.checkDir(fileCharts, true, "File Charts");
		
		Map<String,HdrHistogramSnap> mapHdrHist = new LinkedHashMap<>();
		long tStart = dateStart==null?0:dateStart.getTime();
		long tEnd = dateEnd==null?Long.MAX_VALUE:dateEnd.getTime();
		
		if (tsmHdrHist == null) {
			tsmHdrHist = tvault.openTSMap(TelemetrySchema.TSMAP_HdrHist, Mode.READONLY, -1);
		}
		
		for (TSegmentInfo segInfo : tsmHdrHist.getSegmentMap().values()) {
			LOG.info(String.format("Opening batch %s maxDoc=%d", segInfo.getName(), segInfo.maxDoc()));
			
			if (segInfo.maxDoc() > 0) {
				TSegment seg = tsmHdrHist.loadSegment(segInfo.getName());
				try {
					
					
					IContext c = seg.getContext(null);
					for (int i=0;i<seg.maxDoc();i++) { 
						TRecordDecoder decoder = seg.getDecoder(i, c);
						HdrHistogramSnap snap = new HdrHistogramSnap();
						snap.deserialize(decoder);
						String name = snap.getName();
						boolean fAdd = snap.inRange(tStart,tEnd);
						
						if (fAdd || true) {
							LOG.info(String.format("SnapAgg %s",name));
							HdrHistogramSnap snapAgg = mapHdrHist.computeIfAbsent(name, (_)->{
								String id = String.format("%s_agg_%s", snap.getName(),snap.getRuntimeId());
								return new HdrHistogramSnap(id,snap);
							});
							snapAgg.add(snap);
						} else {
							LOG.info(String.format("Ignoring record %s due to date filter",snap.getId()));
						}
					}

				} finally {
					IOUtils.closeQuietly(seg);
				}
			}

		}
		
		LOG.info(String.format("HdrHistogramSnap %d size.",mapHdrHist.size()));
		
		for (HdrHistogramSnap snap : mapHdrHist.values()) {
			File fileHist = new File(fileCharts,snap.getName()+".hist");
			Validator.checkNewFile(fileHist, true, "hist file");
			String st = snap.getPercentileDist(5, 1.0);
			LOG.info(String.format("Stat:\r\n%s\r\n",st));
			try (FileWriter w = new FileWriter(fileHist,StandardCharsets.UTF_8)) {
				w.write(st);
				w.write("\r\n");
			}
		}
		
	}

	public boolean isClosed() {
		return fClosed;
	}

	public void close() throws IOException {
		fClosed = true;
		if (tsmEvent != null) {
			IOUtils.closeQuietly(tsmEvent);
			tsmEvent = null;
		}

		if (tsmHdrHist != null) {
			IOUtils.closeQuietly(tsmHdrHist);
			tsmHdrHist = null;
		}

		if (tvault != null) {
			tvault.close();
		}
	}
}
