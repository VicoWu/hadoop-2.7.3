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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.Daemon;

import com.google.common.collect.Lists;

/**
 * The Checkpointer is responsible for supporting periodic checkpoints 
 * of the HDFS metadata.
 *
 * The Checkpointer is a daemon that periodically wakes up
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic checkpoint and then goes back to sleep.
 * 
 * The start of a checkpoint is triggered by one of the two factors:
 * (1) time or (2) the size of the edits file.
 */
class Checkpointer extends Daemon {
  public static final Log LOG = 
    LogFactory.getLog(Checkpointer.class.getName());

  private final BackupNode backupNode;
  volatile boolean shouldRun;

  private String infoBindAddress;

  private CheckpointConf checkpointConf;
  private final Configuration conf;

  private BackupImage getFSImage() {
    return (BackupImage)backupNode.getFSImage();
  }

  private NamenodeProtocol getRemoteNamenodeProxy(){
    return backupNode.namenode;
  }

  /**
   * Create a connection to the primary namenode.
   */
  Checkpointer(Configuration conf, BackupNode bnNode)  throws IOException {
    this.conf = conf;
    this.backupNode = bnNode;
    try {
      initialize(conf);
    } catch(IOException e) {
      LOG.warn("Checkpointer got exception", e);
      shutdown();
      throw e;
    }
  }

  /**
   * Initialize checkpoint.
   */
  private void initialize(Configuration conf) throws IOException {
    // Create connection to the namenode.
    shouldRun = true;

    // Initialize other scheduling parameters from the configuration
    checkpointConf = new CheckpointConf(conf);

    // Pull out exact http address for posting url to avoid ip aliasing issues
    String fullInfoAddr = conf.get(DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, 
                                   DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT);
    infoBindAddress = fullInfoAddr.substring(0, fullInfoAddr.indexOf(":"));

    LOG.info("Checkpoint Period : " +
             checkpointConf.getPeriod() + " secs " +
             "(" + checkpointConf.getPeriod()/60 + " min)");
    LOG.info("Transactions count is  : " +
             checkpointConf.getTxnCount() +
             ", to trigger checkpoint");
  }

  /**
   * Shut down the checkpointer.
   */
  void shutdown() {
    shouldRun = false;
    backupNode.stop();
  }

  //
  // The main work loop
  //
  @Override
  public void run() {
    // Check the size of the edit log once every 5 minutes.
    long periodMSec = 5 * 60;   // 5 minutes
    if(checkpointConf.getPeriod() < periodMSec) {
      periodMSec = checkpointConf.getPeriod();
    }
    periodMSec *= 1000;

    long lastCheckpointTime = 0;
    if (!backupNode.shouldCheckpointAtStartup()) {
      lastCheckpointTime = monotonicNow();
    }
    while(shouldRun) {
      try {
        long now = monotonicNow();
        boolean shouldCheckpoint = false;
        if(now >= lastCheckpointTime + periodMSec) {
          shouldCheckpoint = true;
        } else {
          long txns = countUncheckpointedTxns();
          if(txns >= checkpointConf.getTxnCount())
            shouldCheckpoint = true;
        }
        if(shouldCheckpoint) {
          doCheckpoint();
          lastCheckpointTime = now;
        }
      } catch(IOException e) {
        LOG.error("Exception in doCheckpoint: ", e);
      } catch(Throwable e) {
        LOG.error("Throwable Exception in doCheckpoint: ", e);
        shutdown();
        break;
      }
      try {
        Thread.sleep(periodMSec);
      } catch(InterruptedException ie) {
        // do nothing
      }
    }
  }

  private long countUncheckpointedTxns() throws IOException {
    long curTxId = getRemoteNamenodeProxy().getTransactionID();
    long uncheckpointedTxns = curTxId -
      getFSImage().getStorage().getMostRecentCheckpointTxId();
    assert uncheckpointedTxns >= 0;
    return uncheckpointedTxns;
  }

  /**
   * Create a new checkpoint
   */
  void doCheckpoint() throws IOException {
    BackupImage bnImage = getFSImage();
    NNStorage bnStorage = bnImage.getStorage();

    long startTime = monotonicNow();
    bnImage.freezeNamespaceAtNextRoll();
    //服务端调用，查看NameNodeRpcServer.startCheckpoint()，服务端会结束掉正在写的sgement文件，开启一个新的segment文件
    //客户端调用，查看
    NamenodeCommand cmd = 
      getRemoteNamenodeProxy().startCheckpoint(backupNode.getRegistration());
    CheckpointCommand cpCmd = null;
    //关于这些返回值的含义，可以参考FSImage.startCheckpoint()方法
    switch(cmd.getAction()) {
    //If backup storage contains image that is newer than or incompatible with 
    // what the active name-node has, then the backup node should shutdown. wuchang
      case NamenodeProtocol.ACT_SHUTDOWN://如果发现backup node的image比namenode的更新，或者storage的版本不一致，肯定更有问题，这时候backup node需要关闭
        shutdown();
        throw new IOException("Name-node " + backupNode.nnRpcAddress
                                           + " requested shutdown.");
      case NamenodeProtocol.ACT_CHECKPOINT:
        cpCmd = (CheckpointCommand)cmd;
        break;
      default:
        throw new IOException("Unsupported NamenodeCommand: "+cmd.getAction());
    }

    //BackupImage.namenodeStartedLogSegment()如果正在发生，那么如果处于frozen，则必须等待
    bnImage.waitUntilNamespaceFrozen();

    //在进行checkpoint期间，来自rpc的edit只能写入文件，不可以应用到内存
    //查看服务端调用 NameNodeRPCServer.startCheckpoint()
    CheckpointSignature sig = cpCmd.getSignature();

    // Make sure we're talking to the same NN!
    sig.validateStorageInfo(bnImage);

    long lastApplied = bnImage.getLastAppliedTxId();
    LOG.debug("Doing checkpoint. Last applied: " + lastApplied);
    RemoteEditLogManifest manifest = //从NameNode处获取edit log的文件清单列表
      getRemoteNamenodeProxy().getEditLogManifest(bnImage.getLastAppliedTxId() + 1);

    boolean needReloadImage = false;
    if (!manifest.getLogs().isEmpty()) {
      RemoteEditLog firstRemoteLog = manifest.getLogs().get(0);//获取第一个远程的edit log文件
      // we don't have enough logs to roll forward using only logs. Need
      // to download and load the image.
      //如果从远程获取的edit log文件的transaction 与自己目前最后一次已经获取的log文件的transaction存在gap，需要进行reload
      if (firstRemoteLog.getStartTxId() > lastApplied + 1) {
        //sig.mostRecentCheckpointTxId存放了Active NameNode在最后一个checkpoint的位点
        LOG.info("Unable to roll forward using only logs. Downloading " +
            "image with txid " + sig.mostRecentCheckpointTxId);
        MD5Hash downloadedHash = TransferFsImage.downloadImageToStorage(
            backupNode.nnHttpAddress, sig.mostRecentCheckpointTxId, bnStorage,
            true);//从远程获取这个位点的image文件
        bnImage.saveDigestAndRenameCheckpointImage(NameNodeFile.IMAGE,
            sig.mostRecentCheckpointTxId, downloadedHash);
        lastApplied = sig.mostRecentCheckpointTxId;//更新lastApplied id
        needReloadImage = true;
      }

      if (firstRemoteLog.getStartTxId() > lastApplied + 1) {//在下载了最新的image文件以后，依然存在gap，则抛出异常
        throw new IOException("No logs to roll forward from " + lastApplied);
      }
  
      // get edits files
      for (RemoteEditLog log : manifest.getLogs()) {//依次下载这些edit log文件
        TransferFsImage.downloadEditsToStorage(
            backupNode.nnHttpAddress, log, bnStorage);
      }

      //刚才已经下载了新的image文件，因此需要将这个image文件reload到内存
      if(needReloadImage) {
        LOG.info("Loading image with txid " + sig.mostRecentCheckpointTxId);
        File file = bnStorage.findImageFile(NameNodeFile.IMAGE,
            sig.mostRecentCheckpointTxId);
        bnImage.reloadFromImageFile(file, backupNode.getNamesystem());
      }
      rollForwardByApplyingLogs(manifest, bnImage, backupNode.getNamesystem());//将edit log应用到内存
    }
    
    long txid = bnImage.getLastAppliedTxId();
    
    backupNode.namesystem.writeLock();
    try {
      backupNode.namesystem.setImageLoaded();
      if(backupNode.namesystem.getBlocksTotal() > 0) {
        backupNode.namesystem.setBlockTotal();
      }
      bnImage.saveFSImageInAllDirs(backupNode.getNamesystem(), txid);//将当前内存镜像dump到fsimage文件，产生最新的fsimage文件
      bnStorage.writeAll();
    } finally {
      backupNode.namesystem.writeUnlock();
    }

    //将image文件上传给active namenode
    if(cpCmd.needToReturnImage()) {
      TransferFsImage.uploadImageFromStorage(backupNode.nnHttpAddress, conf,
          bnStorage, NameNodeFile.IMAGE, txid);
    }

    getRemoteNamenodeProxy().endCheckpoint(backupNode.getRegistration(), sig);//结束checkpoint过程

    //只有backup 节点需要进行converge操作，追赶txid到最新的状态
    //如果是checkpoint node，没有这种实时性需求，只需要依靠fsimage文件和edit log文件拷贝就可以完成
    //如果是BACKUP,则存在处于in-progress状态的edit log文件，此时需要把这个处于打开状态的文件里面的edit load到内存
    if (backupNode.getRole() == NamenodeRole.BACKUP) {
      bnImage.convergeJournalSpool(); //调用完毕以后，状态成为IN_SYNC，
    }
    backupNode.setRegistration(); // keep registration up to date
    
    long imageSize = bnImage.getStorage().getFsImageName(txid).length();
    LOG.info("Checkpoint completed in "
        + (monotonicNow() - startTime)/1000 + " seconds."
        + " New Image Size: " + imageSize);
  }

  private URL getImageListenAddress() {
    InetSocketAddress httpSocAddr = backupNode.getHttpAddress();
    int httpPort = httpSocAddr.getPort();
    try {
      return new URL(DFSUtil.getHttpClientScheme(conf) + "://" + infoBindAddress + ":" + httpPort);
    } catch (MalformedURLException e) {
      // Unreachable
      throw new RuntimeException(e);
    }
  }

  /**
   * 向前推进txid
   * @param manifest
   * @param dstImage
   * @param dstNamesystem
   * @throws IOException
   */
  static void rollForwardByApplyingLogs(
      RemoteEditLogManifest manifest,
      FSImage dstImage,
      FSNamesystem dstNamesystem) throws IOException {
    NNStorage dstStorage = dstImage.getStorage();
  
    List<EditLogInputStream> editsStreams = Lists.newArrayList();    
    for (RemoteEditLog log : manifest.getLogs()) {
      if (log.getEndTxId() > dstImage.getLastAppliedTxId()) {
        File f = dstStorage.findFinalizedEditsFile(
            log.getStartTxId(), log.getEndTxId());//找到本地的符合这个startTxId和endTxId的editlog文件
        editsStreams.add(new EditLogFileInputStream(f, log.getStartTxId(), 
                                                    log.getEndTxId(), true));
      }
    }
    LOG.info("Checkpointer about to load edits from " +
        editsStreams.size() + " stream(s).");
    dstImage.loadEdits(editsStreams, dstNamesystem);//将这些edit log文件加载到内存
  }
}
