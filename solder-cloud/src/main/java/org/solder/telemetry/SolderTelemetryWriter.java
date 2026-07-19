package org.solder.telemetry;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.DoubleHistogram;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.solder.core.SEvent;
import org.solder.core.SolderException;
import org.solder.vsync.SolderVaultFactory;
import org.solder.vsync.SolderVaultFactory.SRepo;

import com.beech.bfs.Mode;
import com.beech.store.TRecordEncoder;
import com.beech.store.TSMap;
import com.beech.store.TSegmentBuilder;
import com.beech.store.TVault;
import com.ee.session.FileLog;
import com.ee.session.SessionManager;
import com.ee.session.db.ELockProvider;
import com.ee.session.db.Event;
import com.ee.session.db.Machine;
import com.ee.session.db.TelemetrySchema;
import com.ee.util.SessUtil;
import com.jnk.util.PrintUtils;
import com.jnk.util.StopWatchUtil;
import com.jnk.util.Validator;
import com.jnk.util.stats.HdrCounter;
import com.jnk.util.stats.IHdrCounter.HdrType;
import com.lnk.hdr.HdrCounterManager;
import com.lnk.hdr.HdrHistogramSnap;
import com.lnk.lucene.BackgroundTask;
import com.lnk.lucene.FileNameUtil;
import com.lnk.lucene.Lock;
import com.lnk.lucene.RunOnce;
import com.lnk.lucene.util.LogJsonDecoder;

public class SolderTelemetryWriter implements Closeable {

	private static Log LOG = LogFactory.getLog(SolderTelemetryWriter.class.getName());
	static AtomicBoolean s_fInit = new AtomicBoolean(false);

	static final String SCHEMA_NAME_PREFIX = "tmetry_";
	

	// Every Hour or initiate via api call ..

	static final int CACHE_REFRESH_SECONDS = (int) TimeUnit.MINUTES.toSeconds(10);
	
	static private AtomicLong tLastWrite = new AtomicLong(0);
	public static void init() throws IOException {

		//Schedule
		RunOnce.ensure(s_fInit, () -> {
			BackgroundTask.get().createFuture("SolderTelemetry.writeDb", (ee) -> {
				return ee.scheduleWithFixedDelay(SessUtil.makeSessFuture("SolderTelemetry",SolderTelemetryWriter::checkLogging2),
						10, CACHE_REFRESH_SECONDS, TimeUnit.SECONDS);
			});
		});
	}
	
	public static synchronized void checkLogging2() {
		try {
			checkLogging();
			cleanupLogging();
		}catch(Throwable e ) {
			LOG.error("SolderTelemetry error",e);
		}
	}
	
	public static synchronized void cleanupLogging() throws IOException {
		File fileLogRoot = SessionManager.getLogRoot();
		long divisor = TimeUnit.MINUTES.toMillis(60);
		long thresh = ((long)(System.currentTimeMillis()/divisor))*divisor - TimeUnit.DAYS.toMillis(5);
		
		IOConsumer<Path> cCleanup = (path)-> {
		 try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
			 	LOG.info(String.format("Cleanup path %s",path.toString())); 
	            for (Path entry : stream) {
	                // Filter out subdirectories if you only want regular files
	                if (Files.isRegularFile(entry)) {
	                	String name = entry.getFileName().toString();
	                	Date date = FileNameUtil.parseGMTDate(name);
	                	
	                	boolean fFoundDate = date!=null;
	                	boolean fDel = fFoundDate && date.getTime()<thresh;
	                	LOG.info(String.format("checking file %s, fDel=%s, fFoundDate=%s",name,Boolean.toString(fDel),Boolean.toString(fFoundDate)));
	                	if (fDel) {
	                		try {
	                			Files.delete(entry);
	                		}catch(Exception e) {
	                			LOG.info(String.format("Delete Failed %s reason=%s",name,e.toString()));
	                		}
	                	}
	                }
	            }
	        } 
		};
		
		cCleanup.accept(fileLogRoot.toPath());
		File fileLogProcessed = new File(fileLogRoot,"Processed");
		File fileLogDone = new File(fileLogProcessed,"done");
		if (fileLogDone.exists()) {
			cCleanup.accept(fileLogDone.toPath());
		}
		
		
	}
	public static synchronized void generateCharts() throws IOException {
		File fileLogRoot = SessionManager.getLogRoot();
		File fileLogChartRoot = new File(fileLogRoot,"Charts");
		File fileLogCharts = new File(fileLogChartRoot,FileNameUtil.generateGMT("Charts"));
		SolderTelemetryReader reader = SolderTelemetryReader.localReader();
		try {
			reader.generateCharts(null,null,fileLogCharts);
		}finally {
			IOUtils.closeQuietly(reader);
		}
	}
	
	public static synchronized void checkLogging() throws IOException {
		LOG.info(String.format("SolderTelemetry checkLogging"));
		long tThresh = TimeUnit.MINUTES.toMillis(30);
		long tLast = tLastWrite.get();
		boolean fFirst = tLast==0;
		long tNow = System.currentTimeMillis();
		boolean fRun =  fFirst || (tNow-tLast) > tThresh;
		
		//WE can 
		long tEventInitTime = Event.getInitTimeMs();
		
		long tRolloverThresh = FileLog.ROLLOVER_MAX_TIME+TimeUnit.MINUTES.toMillis(10);
		
		LOG.info(String.format("SolderTelemetry checkLogging run=%s(fFirst=%s)",Boolean.toString(fRun),Boolean.toString(fFirst)));
		
		if (fRun) {
			tLastWrite.set(tNow);
			
			File fileLogRoot = SessionManager.getLogRoot();
			File fileLogProcessed = new File(fileLogRoot,"Processed");
			File fileLogDone = new File(fileLogProcessed,"done");
			File fileLogDead = new File(fileLogProcessed,"dead");
			
			Validator.checkDir(fileLogDone, true,"Done dir");
			Validator.checkDir(fileLogDead, true,"Dead dir");
			
			LOG.info(String.format("Listing files %s",fileLogRoot.getAbsolutePath()));
			File[] aFile= fileLogRoot.listFiles(((_,name)->{
				LOG.info(String.format("checking file %s",name));
				if (name.startsWith("event_")) {
					Date date = FileNameUtil.parseGMTDate(name);
					long tFile =-1;
					if (date != null) {
						tFile = date.getTime();
						if (tFile < tEventInitTime || (tNow-tFile)>tRolloverThresh) {
							LOG.info(String.format("Accept %s tFile=%d tEventInitiTime=%d tElapsed=%d tThres=%d",name,tFile,tEventInitTime,(tNow-tFile),tRolloverThresh));
							return true;
						}
					}
					LOG.info(String.format("Reject %s tFile=%d tEventInitiTime=%d tElapsed=%d tThres=%d",name,tFile,tEventInitTime,(tNow-tFile),tRolloverThresh));
				}
				return false;
			}));
			
			LOG.info(String.format("Listing files found %d files.",(aFile!=null?aFile.length:0)));
			
			if (aFile.length>0 || fFirst || (tNow-tLast) > tThresh) {
				
				doTelemetryOp((writer)->{
					LOG.info(String.format("doTelemetryOp EventFiles %d files.",(aFile!=null?aFile.length:0)));
					if (aFile.length>0) {
						for (File file : aFile) {
							try {
								writer.parseEventLog(file);
								File fileMove = new File(fileLogDone,file.getName());
								boolean f = file.renameTo(fileMove);
								LOG.info(String.format("Done Move %s to %s, fSuccess=%s", file.getAbsolutePath(),fileMove.getAbsolutePath(),Boolean.toString(f)));
							}catch(Throwable e) {
								LOG.error((String.format("Error parsing log file %s",file.getAbsolutePath())),e);
								Event.log(SEvent.TelemetryWriterError, -1, -1, (mb) -> {
									mb.put("fn", "parseEventLog");
									mb.put("repoId", writer.repo.getId());
									mb.put("file", file.getAbsolutePath());
									mb.put("error", PrintUtils.getStackTrace(e));
								});
								File fileMove = new File(fileLogDead,file.getName());
								boolean f = file.renameTo(fileMove);
								LOG.info(String.format("Dead Move %s to %s, fSuccess=%s", file.getAbsolutePath(),fileMove.getAbsolutePath(),Boolean.toString(f)));
							}
						}
					}
					writer.snapHdrHistogram();
					
				});
				
				
				
			}
			
			if (fFirst) {
				generateCharts();
			}
			
		} else {
			LOG.info(String.format("SolderTelmetry checkLogging too soon to run!"));
		}
		//Process all old Event Logs...
		
		
	}
	
	
	public static void doTelemetryOp(IOConsumer<SolderTelemetryWriter> cWriter) throws IOException {
		Objects.requireNonNull(cWriter,"cWriter");
		
		LOG.info(String.format("doTelemetryOp"));
		
		Machine machine = Machine.getThis();
		int aoId = machine.getId();
		String dateGroup = getDateGroupId();
		String schemaName = String.format("%s%s", SCHEMA_NAME_PREFIX, dateGroup);

		SRepo repo = SolderVaultFactory.ensureRepo(schemaName, aoId, "active");
		
		String lockName = repo.getId();
		//Get Lock..
		SolderTelemetryWriter writer=null;
		Lock lock = ELockProvider.acquire(lockName, 60*1000L, "TWrite");
		Objects.requireNonNull(lock,"lock");
		try {
			writer = new SolderTelemetryWriter(repo);
			cWriter.accept(writer);
			writer.flush();
			writer.close();
			writer=null;
			Event.log(SEvent.TelemetryWriter, -1, -1, (mb) -> {
				mb.put("repoId", repo.getId());
			});
		}catch(Exception e) {
			Event.log(SEvent.TelemetryWriterError, -1, -1, (mb) -> {
				mb.put("repoId", repo.getId());
				mb.put("error", PrintUtils.getStackTrace(e));
			});
			throw SolderException.rethrow(e);
		}finally {
		
			if (writer!=null) {
				IOUtils.closeQuietly(writer);
			}
			try {
				lock.release();
			}catch(Exception e) {
				LOG.error(String.format("Ignoring Error releasing lock %s", lockName),e);
			}
		}
	}

	public static String getDateGroupId() {
		// Get Current Date..
		LocalDate gmtDate = LocalDate.now(ZoneId.of("GMT"));
		int iYear = gmtDate.getYear();
		int iMonth = gmtDate.getMonthValue();
		int iQuarter = ((iMonth - 1) / 3) + 1;
		return String.format("%04dq%d", iYear, iQuarter);
	}

	
	TVault tvault;
	TSMap tsmEvent, tsmHdrHist;
	SRepo repo;
	SimpleDateFormat sdf;
	List<String> listToFlush;

	SolderTelemetryWriter(SRepo repo) throws IOException {
		// use the pid for now..
		this.repo = Objects.requireNonNull(repo);

		sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		LOG.trace(String.format("GitSync %s loaded", repo.getId()));
		SolderVaultFactory svf = (SolderVaultFactory) TVault.getFactory(SolderVaultFactory.TYPE);
		svf.repoGitPush(repo.getId());

		listToFlush = new ArrayList<>();

		tvault = TVault.open(SolderVaultFactory.TYPE, repo.getId(), Mode.CREATE, TelemetrySchema.INSTANCE);

	}

	@SuppressWarnings("unchecked")
	public void snapHdrHistogram() throws IOException {
		LOG.info(String.format("snapHdrHistogram"));
		if (tsmHdrHist == null) {
			tsmHdrHist = tvault.openTSMap(TelemetrySchema.TSMAP_HdrHist, Mode.WRITE, -1);
		}

		String pid = SessionManager.getPid().toLowerCase();

		String batchName = String.format("%s_%s", pid, sdf.format(new Date()));
		Map<String, String> props = new HashMap<>();
		TSegmentBuilder segBuilder = tsmHdrHist.newBuilder(batchName, props);
		String stRuntimeId = SessionManager.getRuntimeId();
		boolean fCompress = true;

		TRecordEncoder encoder = segBuilder.getEncoder();
		long tNow = System.currentTimeMillis();

		HdrCounterManager.snapshot((hc) -> {

			HdrType htype = hc.getType();
			HdrHistogramSnap hhs = null;
			
			LOG.info(String.format("snapHdrHistogram %s type=%s",hc.getName(),hc.getType().name()));
			
			String id = String.format("%s_%s_%d", hc.getName(),pid,tNow);

			if (htype.isLongHistogram()) {
				HdrCounter<AbstractHistogram> lhc = (HdrCounter<AbstractHistogram>) hc;
				hhs = new HdrHistogramSnap(id,stRuntimeId, lhc, lhc.flush(true), fCompress);
			} else if (htype.isDoubleHistogram()) {
				HdrCounter<DoubleHistogram> lhc = (HdrCounter<DoubleHistogram>) hc;
				hhs = new HdrHistogramSnap(id,stRuntimeId, lhc, lhc.flush(true), fCompress);
			} else {
				// Ignore and move on.. there would be other failure,
				LOG.info(String.format("Ignore error; unknown type %s", htype.name()));
			}

			encoder.clear();
			hhs.serialize(encoder);
			segBuilder.add(encoder);
		});

		segBuilder.commitWrite();
		listToFlush.add(batchName);

	}

	public void parseEventLog(File file) throws IOException {
		Validator.checkFile(file, "EventLogFile");
		LOG.info(String.format("Parsing event log %s size=%,d", file.getAbsolutePath(), file.length()));

		String logName = file.getName();
		if (logName.startsWith("event_")) {
			// remove it..
			logName = logName.substring("event_".length());
		}
		if (tsmEvent == null) {
			tsmEvent = tvault.openTSMap(TelemetrySchema.TSMAP_EVENT, Mode.WRITE, -1);
		}

		String pid = SessionManager.getPid().toLowerCase();

		String batchName = String.format("%s_%s", pid, logName);
		LOG.info(String.format("Creating Batch %s (pid=%s logName=%s)",batchName,pid,logName));
		if (tsmEvent.checkBatchExists(batchName, false)) {
			LOG.info(String.format("Event batch %s exists.(Probably imported), Skipping file %s", batchName,
					file.getAbsolutePath()));
			return;
		}

		if (file.length() <= 10) {
			// Even 1 byte file makes no sense..
			LOG.info(String.format("File too small probably abondoned"));

			return;
		}
		Map<String, String> props = new HashMap<>();
		TSegmentBuilder segBuilder = tsmEvent.newBuilder(batchName, props);
		TRecordEncoder encoder = segBuilder.getEncoder();

		AtomicInteger aiLines = new AtomicInteger(0);
		AtomicInteger aiErrors = new AtomicInteger(0);
		LogJsonDecoder ljd = LogJsonDecoder.getTL();
		boolean fSuccess=false;
		try (Stream<String> lines = Files.lines(file.toPath())) {
			lines.forEach((st) -> {
				try {
					ljd.readJson(st, (jd)->{
						Event event = jd.readObject(Event.class);
						encoder.clear();
						event.serialize(encoder);
						segBuilder.add(encoder);
					});
					aiLines.incrementAndGet();
				} catch(Exception e) {
					int nErrors = aiErrors.incrementAndGet();
					LOG.info("Ignore log parsing error",e);
					if (nErrors>10) {
						throw new RuntimeException("Error parsing "+file.getAbsolutePath());
					}
					
				}finally {
				}
			});
			LOG.info(String.format("Imported %s nRecs=%d (nExpect=%d), nErrors=%d",file.getName(), segBuilder.size(),aiLines.get(),aiErrors.get()));
			if (segBuilder.size() != aiLines.get()) {
				throw new SolderException(String.format("Size mismatch got %d (expect %d)",segBuilder.size(),aiLines.get()));
			}
			fSuccess = true;
			segBuilder.commitWrite();
			listToFlush.add(batchName);
		} finally {
			if (!fSuccess) {
				IOUtils.closeQuietly(segBuilder);
			}
		}

	}

	public void flush() throws IOException {

		// Open only when we write events.
		StopWatch sw = StopWatchUtil.makeSwatch("TFlush");
		sw.resume();
		if (tsmEvent != null) {
			tsmEvent.snap();
			tsmEvent.close();
			tsmEvent = null;
		}

		if (tsmHdrHist != null) {
			tsmHdrHist.snap();
			tsmHdrHist.close();
			tsmHdrHist = null;
		}

		if (tvault != null) {
			tvault.close();

		}
		sw.suspend();
		String stBatch = StringUtils.join(listToFlush, ",");
		LOG.info(String.format("Flushed batch %s on repo %s; Took %s", stBatch, repo.getId(), sw.formatTime()));
		SolderVaultFactory svf = (SolderVaultFactory) TVault.getFactory(SolderVaultFactory.TYPE);
		svf.repoGitPush(repo.getId());
		LOG.info(String.format("GitPush Repo %s ; Took %s", repo.getId(), sw.formatTime()));
		listToFlush.clear();
	}

	public void close() throws IOException {

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
