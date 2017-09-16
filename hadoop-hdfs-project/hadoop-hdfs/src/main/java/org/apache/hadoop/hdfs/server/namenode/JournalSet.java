/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import static org.apache.hadoop.util.ExitUtil.terminate;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

/**
 * Manages a collection of Journals. None of the methods are synchronized, it is
 * assumed that FSEditLog methods, that use this class, use proper
 * synchronization.
 */
public class JournalSet implements JournalManager {

  static final Log LOG = LogFactory.getLog(FSEditLog.class);

  private static final Comparator<EditLogInputStream>
    LOCAL_LOG_PREFERENCE_COMPARATOR = new Comparator<EditLogInputStream>() {
    @Override
    public int compare(EditLogInputStream elis1, EditLogInputStream elis2) {
      // we want local logs to be ordered earlier in the collection, and true
      // is considered larger than false, so we want to invert the booleans here
      return ComparisonChain.start().compare(!elis1.isLocalLog(),
          !elis2.isLocalLog()).result();
    }
  };
  

  public static void main(String[] args){
	   Comparator<Integer>
	    testComparator = new Comparator<Integer>() {
	    @Override
	    public int compare(Integer a, Integer b) {
	      // we want local logs to be ordered earlier in the collection, and true
	      // is considered larger than false, so we want to invert the booleans here
	      return ComparisonChain.start().compare(!(a<5),
	          !(b<5)).result();
	    }
	  };
	  List<Integer> list =new LinkedList<Integer>();
	  list.add(1);
	  list.add(7);
	  list.add(2);
	  list.add(8);
	  Collections.sort(list, testComparator);
	  for(Integer i:list){
		  System.out.println(i);
	  }
  }
  static final public Comparator<EditLogInputStream>
    EDIT_LOG_INPUT_STREAM_COMPARATOR = new Comparator<EditLogInputStream>() {
      @Override
      public int compare(EditLogInputStream a, EditLogInputStream b) {
        return ComparisonChain.start().
          compare(a.getFirstTxId(), b.getFirstTxId()).
          compare(b.getLastTxId(), a.getLastTxId()).
          result();
      }
    };
  
  /**
   * Container for a JournalManager paired with its currently
   * active stream.
   * 
   * If a Journal gets disabled due to an error writing to its
   * stream, then the stream will be aborted and set to null.
   * 这个JournalAndStream维护了一个JournalManager和一个Stream的对应关系
   * ，即封装了一个JournalManager和一个EditLogOutputStream的实现类
   */
  static class JournalAndStream implements CheckableNameNodeResource {
    private final JournalManager journal; //对应的JournalManager
    private boolean disabled = false;
    private EditLogOutputStream stream;//可能是一个QuorumOutputStream，或者是一个EditLogFileOutputStream
    private final boolean required;
    private final boolean shared;
    
    public JournalAndStream(JournalManager manager, boolean required,
        boolean shared) {
      this.journal = manager;
      this.required = required;
      this.shared = shared;
    }

    //在JournalSet.startLogSegment中会调用JournalAndStream.startLogSegment()方法，方法中
    //会创建一个QuorumOutputStream或者EditLogFileOutputStream的strema用来对edit文件进行写操作
    public void startLogSegment(long txId, int layoutVersion) throws IOException {
      Preconditions.checkState(stream == null);
      disabled = false;
      //如果是QuorumJournalManager，则stream是一个QuorumOutputStream
      //如果是FileJournalManager，则stream是一个EditLogFileOutputStream
      stream = journal.startLogSegment(txId, layoutVersion);
    }

    /**
     * Closes the stream, also sets it to null.
     */
    public void closeStream() throws IOException {
      if (stream == null) return;
      stream.close();
      stream = null;
    }

    /**
     * Close the Journal and Stream
     */
    public void close() throws IOException {
      closeStream();

      journal.close();
    }
    
    /**
     * Aborts the stream, also sets it to null.
     */
    public void abort() {
      if (stream == null) return;
      try {
        stream.abort();
      } catch (IOException ioe) {
        LOG.error("Unable to abort stream " + stream, ioe);
      }
      stream = null;
    }

    boolean isActive() {
      return stream != null;
    }
    
    /**
     * Should be used outside JournalSet only for testing.
     */
    EditLogOutputStream getCurrentStream() {
      return stream;
    }
    
    @Override
    public String toString() {
      return "JournalAndStream(mgr=" + journal +
        ", " + "stream=" + stream + ")";
    }

    void setCurrentStreamForTests(EditLogOutputStream stream) {
      this.stream = stream;
    }
    
    JournalManager getManager() {
      return journal;
    }

    boolean isDisabled() {
      return disabled;
    }

    private void setDisabled(boolean disabled) {
      this.disabled = disabled;
    }
    
    @Override
    public boolean isResourceAvailable() {
      return !isDisabled();
    }
    
    @Override
    public boolean isRequired() {
      return required;
    }
    
    public boolean isShared() {
      return shared;
    }
  }
 
  // COW implementation is necessary since some users (eg the web ui) call
  // getAllJournalStreams() and then iterate. Since this is rarely
  // mutated, there is no performance concern.
  //维护了所有的JournalAndStream对象,通过FSEdit类中调用initJournals方法进行添加的
  private final List<JournalAndStream> journals =
      new CopyOnWriteArrayList<JournalSet.JournalAndStream>();
  final int minimumRedundantJournals;

  private boolean closed;
  
  JournalSet(int minimumRedundantResources) {
    this.minimumRedundantJournals = minimumRedundantResources;
  }
  
  @Override
  public void format(NamespaceInfo nsInfo) throws IOException {
    // The operation is done by FSEditLog itself
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasSomeData() throws IOException {
    // This is called individually on the underlying journals,
    // not on the JournalSet.
    throw new UnsupportedOperationException();
  }

  
  /**
   * 在JournalSet维护的所有的journal上面执行apply，即对所有的journal执行startLogSegment
   * 然后返回一个JournalSetOutputStream，以后FSEditLog调用写等操作，都交付给JournalSetOutputStream
   * ，JournalSetOutputStream会负责在所有的journal上都执行这个操作
   */
  
  @Override
  public EditLogOutputStream startLogSegment(final long txId,
      final int layoutVersion) throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.startLogSegment(txId, layoutVersion);
      }
    }, "starting log segment " + txId);
    return new JournalSetOutputStream();
  }
  
  /**
   * 用来告知远程的JournalNode，对应的某个segment可以进行关闭操作了。
   */
  @Override
  public void finalizeLogSegment(final long firstTxId, final long lastTxId)
      throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        if (jas.isActive()) {
          jas.closeStream();
          //最终调用对应的JournalManager的finalizeLogSegment,这个方法会向远程的JournalServer发送关闭请求
          jas.getManager().finalizeLogSegment(firstTxId, lastTxId);
        }
      }
    }, "finalize log segment " + firstTxId + ", " + lastTxId);
  }
   
  @Override
  public void close() throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.close();
      }
    }, "close journal");
    closed = true;
  }

  public boolean isOpen() {
    return !closed;
  }

  /**
   * In this function, we get a bunch of streams from all of our JournalManager
   * objects.  Then we add these to the collection one by one.
   * 
   * @param streams          The collection to add the streams to.  It may or 
   *                         may not be sorted-- this is up to the caller.
   * @param fromTxId         The transaction ID to start looking for streams at
   * @param inProgressOk     Should we consider unfinalized streams?
   */
  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxId, boolean inProgressOk) throws IOException {
	//这个allStreams中保存了对本地和远程的读取流，这里的stream经过了EDIT_LOG_INPUT_STREAM_COMPARATOR定义的顺序进行了排序，
	  //start txId越小越靠前，end txId越大越靠前
    final PriorityQueue<EditLogInputStream> allStreams = 
        new PriorityQueue<EditLogInputStream>(64,
            EDIT_LOG_INPUT_STREAM_COMPARATOR);
    for (JournalAndStream jas : journals) {
      if (jas.isDisabled()) {
        LOG.info("Skipping jas " + jas + " since it's disabled");
        continue;
      }
      try {
    	//这里的manager既包括了本地的FileJournalManager,也包括了远程的QuorumJournalManager
        jas.getManager().selectInputStreams(allStreams, fromTxId, inProgressOk);
      } catch (IOException ioe) {
        LOG.warn("Unable to determine input streams from " + jas.getManager() +
            ". Skipping.", ioe);
      }
    }
    //注意，allStreams已经进行了排序，EDIT_LOG_INPUT_STREAM_COMPARATOR定义的顺序进行了排序，
	  //start txId越小越靠前，end txId越大越靠前
    chainAndMakeRedundantStreams(streams, allStreams, fromTxId);
  }
  
  /**
   * 这个方法会遍历目前所有的stream，将这些stream进行排序，将本地的stream排在远程的stream前面
   * 同时，有可能某些stream的start txId相同，这时候，会为这些stream构造出一个RedundantEditLogInputStream，
   * 即做了一个副本stream，这样，读取的时候，万一某个stream出现问题，可以使用另外一个或者多个副本进行读取，同样，RedundantEditLogInputStream
   * 中维护的多个互为副本的stream，也是遵循本地stream优先的
   * @param outStreams
   * @param allStreams 实现是EditLogFileInputStream
   * @param fromTxId
   * 注意，allStreams已经进行了排序，EDIT_LOG_INPUT_STREAM_COMPARATOR定义的顺序进行了排序，
	 start txId越小越靠前，end txId越大越靠前
	 
   */
  public static void chainAndMakeRedundantStreams(
      Collection<EditLogInputStream> outStreams,
      PriorityQueue<EditLogInputStream> allStreams, long fromTxId) {
    // We want to group together all the streams that start on the same start
    // transaction ID.  To do this, we maintain an accumulator (acc) of all
    // the streams we've seen at a given start transaction ID.  When we see a
    // higher start transaction ID, we select a stream from the accumulator and
    // clear it.  Then we begin accumulating streams with the new, higher start
    // transaction ID.
    LinkedList<EditLogInputStream> acc =
        new LinkedList<EditLogInputStream>();
    EditLogInputStream elis;
    while ((elis = allStreams.poll()) != null) {
      if (acc.isEmpty()) {
        acc.add(elis);
      } else {
        EditLogInputStream accFirst = acc.get(0);
        long accFirstTxId = accFirst.getFirstTxId();
        if (accFirstTxId == elis.getFirstTxId()) {
          // if we have a finalized log segment available at this txid,
          // we should throw out all in-progress segments at this txid
          if (elis.isInProgress()) {
            if (accFirst.isInProgress()) {
              acc.add(elis);
            }
          } else {//当前的这个stream文件是一个finalized segment
            if (accFirst.isInProgress()) {
              acc.clear();//丢弃所有处于in-progress的文件
            }
            acc.add(elis);//添加这个处于finalized的文件
          }
        } else if (accFirstTxId < elis.getFirstTxId()) {
          // try to read from the local logs first since the throughput should
          // be higher
          Collections.sort(acc, LOCAL_LOG_PREFERENCE_COMPARATOR);
          //将acc拷贝了一份出来，转换成RedundantEditLogInputStream，加入到outStreams中
          outStreams.add(new RedundantEditLogInputStream(acc, fromTxId));
          acc.clear();//清空当前的列表
          acc.add(elis);//把当前的stream加入到acc中，也就是从这个txId更大的stream开始
        } else if (accFirstTxId > elis.getFirstTxId()) {
          throw new RuntimeException("sorted set invariants violated!  " +
              "Got stream with first txid " + elis.getFirstTxId() +
              ", but the last firstTxId was " + accFirstTxId);
        }
      }
    }
    if (!acc.isEmpty()) {
      Collections.sort(acc, LOCAL_LOG_PREFERENCE_COMPARATOR);
      outStreams.add(new RedundantEditLogInputStream(acc, fromTxId));
      acc.clear();
    }
  }

  /**
   * Returns true if there are no journals, all redundant journals are disabled,
   * or any required journals are disabled.
   * 
   * @return True if there no journals, all redundant journals are disabled,
   * or any required journals are disabled.
   */
  public boolean isEmpty() {
    return !NameNodeResourcePolicy.areResourcesAvailable(journals,
        minimumRedundantJournals);
  }
  
  /**
   * Called when some journals experience an error in some operation.
   */
  private void disableAndReportErrorOnJournals(List<JournalAndStream> badJournals) {
    if (badJournals == null || badJournals.isEmpty()) {
      return; // nothing to do
    }
 
    for (JournalAndStream j : badJournals) {
      LOG.error("Disabling journal " + j);
      j.abort();
      j.setDisabled(true);
    }
  }

  /**
   * Implementations of this interface encapsulate operations that can be
   * iteratively applied on all the journals. For example see
   * {@link JournalSet#mapJournalsAndReportErrors}.
   */
  private interface JournalClosure {
    /**
     * The operation on JournalAndStream.
     * @param jas Object on which operations are performed.
     * @throws IOException
     */
    public void apply(JournalAndStream jas) throws IOException;
  }
  
  /**
   * Apply the given operation across all of the journal managers, disabling
   * any for which the closure throws an IOException.
   * @param closure {@link JournalClosure} object encapsulating the operation.
   * @param status message used for logging errors (e.g. "opening journal")
   * @throws IOException If the operation fails on all the journals.
   * 在所有的journalManaer上调用apply方法
   */
  private void mapJournalsAndReportErrors(
      JournalClosure closure, String status) throws IOException{

    List<JournalAndStream> badJAS = Lists.newLinkedList();
    for (JournalAndStream jas : journals) {
      try {
        closure.apply(jas);
      } catch (Throwable t) {
        if (jas.isRequired()) {
          final String msg = "Error: " + status + " failed for required journal ("
            + jas + ")";
          LOG.fatal(msg, t);
          // If we fail on *any* of the required journals, then we must not
          // continue on any of the other journals. Abort them to ensure that
          // retry behavior doesn't allow them to keep going in any way.
          abortAllJournals();
          // the current policy is to shutdown the NN on errors to shared edits
          // dir. There are many code paths to shared edits failures - syncs,
          // roll of edits etc. All of them go through this common function 
          // where the isRequired() check is made. Applying exit policy here 
          // to catch all code paths.
          terminate(1, msg);
        } else {
          LOG.error("Error: " + status + " failed for (journal " + jas + ")", t);
          badJAS.add(jas);          
        }
      }
    }
    disableAndReportErrorOnJournals(badJAS);
    if (!NameNodeResourcePolicy.areResourcesAvailable(journals,
        minimumRedundantJournals)) {
      String message = status + " failed for too many journals";
      LOG.error("Error: " + message);
      throw new IOException(message);
    }
  }
  
  /**
   * Abort all of the underlying streams.
   */
  private void abortAllJournals() {
    for (JournalAndStream jas : journals) {
      if (jas.isActive()) {
        jas.abort();
      }
    }
  }

  /**
   * An implementation of EditLogOutputStream that applies a requested method on
   * all the journals that are currently active.
   * JournalSetOutputStream只是一个装饰器，用来在FSEditLog发起某个操作的时候，将这个操作应用到所有的 Journal上面
   */
  private class JournalSetOutputStream extends EditLogOutputStream {

    JournalSetOutputStream() throws IOException {
      super();
    }

    @Override
    public void write(final FSEditLogOp op)
        throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().write(op);
          }
        }
      }, "write op");
    }

    @Override
    public void writeRaw(final byte[] data, final int offset, final int length)
        throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().writeRaw(data, offset, length);
          }
        }
      }, "write bytes");
    }

    @Override
    public void create(final int layoutVersion) throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().create(layoutVersion);
          }
        }
      }, "create");
    }

    @Override
    public void close() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          jas.closeStream();
        }
      }, "close");
    }

    @Override
    public void abort() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          jas.abort();
        }
      }, "abort");
    }

    @Override
    /**
     * 对journals中的每一个，调用.getCurrentStream().setReadyToFlush()操作
     */
    public void setReadyToFlush() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().setReadyToFlush();
          }
        }
      }, "setReadyToFlush");
    }

    @Override
    protected void flushAndSync(final boolean durable) throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().flushAndSync(durable);
          }
        }
      }, "flushAndSync");
    }
    
    @Override
    public void flush() throws IOException {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
        	//jas是一个JournalAndStream对象
            jas.getCurrentStream().flush();
          }
        }
      }, "flush");
    }
    
    @Override
    public boolean shouldForceSync() {
      for (JournalAndStream js : journals) {
        if (js.isActive() && js.getCurrentStream().shouldForceSync()) {
          return true;
        }
      }
      return false;
    }
    
    @Override
    protected long getNumSync() {
      for (JournalAndStream jas : journals) {
        if (jas.isActive()) {
          return jas.getCurrentStream().getNumSync();
        }
      }
      return 0;
    }
  }

  @Override
  public void setOutputBufferCapacity(final int size) {
    try {
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
            jas.getManager().setOutputBufferCapacity(size);
        }
      }, "setOutputBufferCapacity");
    } catch (IOException e) {
      LOG.error("Error in setting outputbuffer capacity");
    }
  }
  
  List<JournalAndStream> getAllJournalStreams() {
    return journals;
  }

  List<JournalManager> getJournalManagers() {
    List<JournalManager> jList = new ArrayList<JournalManager>();
    for (JournalAndStream j : journals) {
      jList.add(j.getManager());
    }
    return jList;
  }
  
  void add(JournalManager j, boolean required) {
    add(j, required, false);
  }
  
  //添加一个JournalStream对象，一个JournalAndStream对象封装了一个JournalManager和一个EditLogOutputStream的实现类
  void add(JournalManager j, boolean required, boolean shared) {
	//根据JournalManager构造一个JournalAndStream对象
    JournalAndStream jas = new JournalAndStream(j, required, shared);
    journals.add(jas);
  }
  
  void remove(JournalManager j) {
    JournalAndStream jasToRemove = null;
    for (JournalAndStream jas: journals) {
      if (jas.getManager().equals(j)) {
        jasToRemove = jas;
        break;
      }
    }
    if (jasToRemove != null) {
      jasToRemove.abort();
      journals.remove(jasToRemove);
    }
  }

  @Override
  public void purgeLogsOlderThan(final long minTxIdToKeep) throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.getManager().purgeLogsOlderThan(minTxIdToKeep);
      }
    }, "purgeLogsOlderThan " + minTxIdToKeep);
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.getManager().recoverUnfinalizedSegments();
      }
    }, "recoverUnfinalizedSegments");
  }
  
  /**
   * Return a manifest of what finalized edit logs are available. All available
   * edit logs are returned starting from the transaction id passed. If
   * 'fromTxId' falls in the middle of a log, that log is returned as well.
   * 
   * @param fromTxId Starting transaction id to read the logs.
   * @return RemoteEditLogManifest object.
   */
  public synchronized RemoteEditLogManifest getEditLogManifest(long fromTxId) {
    // Collect RemoteEditLogs available from each FileJournalManager
    List<RemoteEditLog> allLogs = Lists.newArrayList();
    for (JournalAndStream j : journals) {
      if (j.getManager() instanceof FileJournalManager) {
        FileJournalManager fjm = (FileJournalManager)j.getManager();
        try {
          allLogs.addAll(fjm.getRemoteEditLogs(fromTxId, false));
        } catch (Throwable t) {
          LOG.warn("Cannot list edit logs in " + fjm, t);
        }
      }
    }
    
    // Group logs by their starting txid
    ImmutableListMultimap<Long, RemoteEditLog> logsByStartTxId =
      Multimaps.index(allLogs, RemoteEditLog.GET_START_TXID);
    long curStartTxId = fromTxId;

    List<RemoteEditLog> logs = Lists.newArrayList();
    while (true) {
      ImmutableList<RemoteEditLog> logGroup = logsByStartTxId.get(curStartTxId);
      if (logGroup.isEmpty()) {
        // we have a gap in logs - for example because we recovered some old
        // storage directory with ancient logs. Clear out any logs we've
        // accumulated so far, and then skip to the next segment of logs
        // after the gap.
        SortedSet<Long> startTxIds = Sets.newTreeSet(logsByStartTxId.keySet());
        startTxIds = startTxIds.tailSet(curStartTxId);
        if (startTxIds.isEmpty()) {
          break;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found gap in logs at " + curStartTxId + ": " +
                "not returning previous logs in manifest.");
          }
          logs.clear();
          curStartTxId = startTxIds.first();
          continue;
        }
      }

      // Find the one that extends the farthest forward
      RemoteEditLog bestLog = Collections.max(logGroup);
      logs.add(bestLog);
      // And then start looking from after that point
      curStartTxId = bestLog.getEndTxId() + 1;
    }
    RemoteEditLogManifest ret = new RemoteEditLogManifest(logs);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated manifest for logs since " + fromTxId + ":"
          + ret);      
    }
    return ret;
  }

  /**
   * Add sync times to the buffer.
   */
  String getSyncTimes() {
    StringBuilder buf = new StringBuilder();
    for (JournalAndStream jas : journals) {
      if (jas.isActive()) {
        buf.append(jas.getCurrentStream().getTotalSyncTime());
        buf.append(" ");
      }
    }
    return buf.toString();
  }

  @Override
  public void discardSegments(long startTxId) throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }

  @Override
  public void doPreUpgrade() throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }

  @Override
  public void doUpgrade(Storage storage) throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void doFinalize() throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage, int targetLayoutVersion) throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }

  @Override
  public void doRollback() throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }

  @Override
  public long getJournalCTime() throws IOException {
    // This operation is handled by FSEditLog directly.
    throw new UnsupportedOperationException();
  }
}
