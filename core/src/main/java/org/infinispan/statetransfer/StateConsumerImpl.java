/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.context.Flag.*;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * {@link StateConsumer} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateConsumerImpl implements StateConsumer {

   private static final Log log = LogFactory.getLog(StateConsumerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private ExecutorService executorService;
   private StateTransferManager stateTransferManager;
   private String cacheName;
   private Configuration configuration;
   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private TransactionTable transactionTable;       // optional
   private DataContainer dataContainer;
   private CacheLoaderManager cacheLoaderManager;   // optional
   private InterceptorChain interceptorChain;
   private InvocationContextContainer icc;
   private StateTransferLock stateTransferLock;
   private long timeout;
   private boolean useVersionedPut;
   private boolean fetchEnabled;
   private boolean isTransactional;

   private volatile CacheTopology cacheTopology;

   /**
    * The number of topology updates that are being processed concurrently (in method onTopologyUpdate()).
    * This is needed to be able to detect completion.
    */
   private AtomicInteger activeTopologyUpdates = new AtomicInteger(0);

   /**
    * Indicates if the currently executing topology update is a rebalance.
    */
   private AtomicBoolean rebalanceInProgress = new AtomicBoolean(false);

   /**
    * A map that keeps track of current inbound state transfers by source address. There could be multiple transfers
    * flowing in from the same source (but for different segments) so the values are lists. This works in tandem with
    * transfersBySegment so they always need to be kept in sync and updates to both of them need to be atomic.
    */
   private Map<Address, List<InboundTransferTask>> transfersBySource = new HashMap<Address, List<InboundTransferTask>>();

   /**
    * A map that keeps track of current inbound state transfers by segment id. There is at most one transfers per segment.
    * This works in tandem with transfersBySource so they always need to be kept in sync and updates to both of them
    * need to be atomic.
    */
   private Map<Integer, InboundTransferTask> transfersBySegment = new HashMap<Integer, InboundTransferTask>();

   public StateConsumerImpl() {
   }

   @Inject
   public void init(Cache cache,
                    @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService, //TODO Use a dedicated ExecutorService
                    StateTransferManager stateTransferManager,
                    InterceptorChain interceptorChain,
                    InvocationContextContainer icc,
                    Configuration configuration,
                    RpcManager rpcManager,
                    CommandsFactory commandsFactory,
                    CacheLoaderManager cacheLoaderManager,
                    DataContainer dataContainer,
                    TransactionTable transactionTable,
                    StateTransferLock stateTransferLock) {
      this.cacheName = cache.getName();
      this.executorService = executorService;
      this.stateTransferManager = stateTransferManager;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.cacheLoaderManager = cacheLoaderManager;
      this.dataContainer = dataContainer;
      this.transactionTable = transactionTable;
      this.stateTransferLock = stateTransferLock;

      isTransactional = configuration.transaction().transactionMode().isTransactional();

      // we need to use a special form of PutKeyValueCommand that can apply versions too
      useVersionedPut = isTransactional &&
            configuration.versioning().enabled() &&
            configuration.locking().writeSkewCheck() &&
            configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC &&
            configuration.clustering().cacheMode().isClustered();

      timeout = configuration.clustering().stateTransfer().timeout();
   }

   public boolean isStateTransferInProgress() {
      // TODO This is called quite often, use an extra volatile, a concurrent collection or a RWLock instead
      synchronized (this) {
         return !transfersBySource.isEmpty();
      }
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      if (configuration.clustering().cacheMode().isInvalidation()) {
         return false;
      }
      // todo [anistor] also return true for keys to be removed (now we report only keys to be added)
      synchronized (this) {
         return cacheTopology != null && transfersBySegment.containsKey(getSegment(key));
      }
   }

   @Override
   public void onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance) {
      if (trace) log.tracef("Received new CH %s for cache %s", cacheTopology.getWriteConsistentHash(), cacheName);

      activeTopologyUpdates.incrementAndGet();
      if (isRebalance) {
         rebalanceInProgress.set(true);
      }
      final ConsistentHash previousCh = this.cacheTopology != null ? this.cacheTopology.getWriteConsistentHash() : null;
      // Ensures writes to the data container use the right consistent hash
      // No need for a try/finally block, since it's just an assignment
      stateTransferLock.acquireExclusiveTopologyLock();
      this.cacheTopology = cacheTopology;
      stateTransferLock.releaseExclusiveTopologyLock();
      stateTransferLock.notifyTopologyInstalled(cacheTopology.getTopologyId());

      try {
         // fetch transactions and data segments from other owners if this is enabled
         if (isTransactional || fetchEnabled) {
            Set<Integer> addedSegments;
            if (previousCh == null) {
               // we start fresh, without any data, so we need to pull everything we own according to writeCh

               addedSegments = getOwnedSegments(cacheTopology.getWriteConsistentHash());

               if (trace) {
                  log.tracef("On cache %s we have: added segments: %s", cacheName, addedSegments);
               }
            } else {
               Set<Integer> previousSegments = getOwnedSegments(previousCh);
               Set<Integer> newSegments = getOwnedSegments(cacheTopology.getWriteConsistentHash());

               // we need to diff the routing tables of the two CHes
               Set<Integer> removedSegments = new HashSet<Integer>(previousSegments);
               removedSegments.removeAll(newSegments);

               addedSegments = new HashSet<Integer>(newSegments);
               addedSegments.removeAll(previousSegments);

               if (trace) {
                  log.tracef("On cache %s we have: removed segments: %s; new segments: %s; old segments: %s; added segments: %s",
                        cacheName, removedSegments, newSegments, previousSegments, addedSegments);
               }

               // remove inbound transfers and any data for segments we no longer own
               discardSegments(removedSegments);

               // check if any of the existing transfers should be restarted from a different source because the initial source is no longer a member
               Set<Address> members = new HashSet<Address>(cacheTopology.getReadConsistentHash().getMembers());
               synchronized (this) {
                  for (Iterator<Address> it = transfersBySource.keySet().iterator(); it.hasNext(); ) {
                     Address source = it.next();
                     if (!members.contains(source)) {
                        if (trace) {
                           log.tracef("Removing inbound transfers from source %s for cache %s", source, cacheName);
                        }
                        List<InboundTransferTask> inboundTransfers = transfersBySource.get(source);
                        it.remove();
                        for (InboundTransferTask inboundTransfer : inboundTransfers) {
                           // these segments will be restarted if they are still in new write CH
                           if (trace) {
                              log.tracef("Removing inbound transfers for segments %s from source %s for cache %s", inboundTransfer.getSegments(), source, cacheName);
                           }
                           transfersBySegment.keySet().removeAll(inboundTransfer.getSegments());
                        }
                     }
                  }

                  // exclude those that are already in progress from a valid source
                  addedSegments.removeAll(transfersBySegment.keySet());
               }
            }

            if (!addedSegments.isEmpty()) {
               // the set of nodes that reported errors when fetching data from them - these will not be retried in this topology
               Set<Address> excludedSources = new HashSet<Address>();
               Map<Address, Set<Integer>> sources = new HashMap<Address, Set<Integer>>();

               if (isTransactional) {
                  requestTransactions(addedSegments, sources, excludedSources);
               }

               // request the segments
               if (fetchEnabled) {
                  requestSegments(addedSegments, sources, excludedSources);
               }
            }
         }
      } finally {
         stateTransferLock.notifyTransactionDataReceived(cacheTopology.getTopologyId());

         if (activeTopologyUpdates.decrementAndGet() == 0) {
            notifyEndOfTopologyUpdate(cacheTopology.getTopologyId());
         }
      }
   }

   private void notifyEndOfTopologyUpdate(int topologyId) {
      if (!isStateTransferInProgress()) {
         if (rebalanceInProgress.compareAndSet(true, false)) {
            log.debugf("Finished receiving of segments for cache %s for topology %d.", cacheName, topologyId);
            stateTransferManager.notifyEndOfTopologyUpdate(topologyId);
         }
      }
   }

   private Set<Integer> getOwnedSegments(ConsistentHash consistentHash) {
      Address address = rpcManager.getAddress();
      return consistentHash.getMembers().contains(address) ? consistentHash.getSegmentsForOwner(address)
            : InfinispanCollections.<Integer>emptySet();
   }

   public void applyState(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
      for (StateChunk stateChunk : stateChunks) {
         // it's possible to receive a late message so we must be prepared to ignore segments we no longer own
         //todo [anistor] this check should be based on topologyId
         if (!cacheTopology.getWriteConsistentHash().getSegmentsForOwner(rpcManager.getAddress()).contains(stateChunk.getSegmentId())) {
            if (trace) {
               log.warnf("Discarding received cache entries for segment %d of cache %s because they do not belong to this node.", stateChunk.getSegmentId(), cacheName);
            }
            continue;
         }

         // notify the inbound task that a chunk of cache entries was received
         InboundTransferTask inboundTransfer;
         synchronized (this) {
            inboundTransfer = transfersBySegment.get(stateChunk.getSegmentId());
         }
         if (inboundTransfer != null) {
            if (stateChunk.getCacheEntries() != null) {
               doApplyState(sender, stateChunk.getSegmentId(), stateChunk.getCacheEntries());
            }

            inboundTransfer.onStateReceived(stateChunk.getSegmentId(), stateChunk.isLastChunk());
         } else {
            log.warnf("Received unsolicited state from node %s for segment %d of cache %s", sender, stateChunk.getSegmentId(), cacheName);
         }
      }

      if (trace) {
         log.tracef("After applying the received state the data container of cache %s has %d keys", cacheName, dataContainer.size());
         synchronized (this) {
            log.tracef("Segments not received yet for cache %s: %s", cacheName, transfersBySource);
         }
      }
   }

   private void doApplyState(Address sender, int segmentId, Collection<InternalCacheEntry> cacheEntries) {
      log.debugf("Applying new state for segment %d of cache %s from node %s: received %d cache entries", segmentId, cacheName, sender, cacheEntries.size());
      if (trace) {
         List<Object> keys = new ArrayList<Object>(cacheEntries.size());
         for (InternalCacheEntry e : cacheEntries) {
            keys.add(e.getKey());
         }
         log.tracef("Received keys %s for segment %d of cache %s from node %s", keys, segmentId, cacheName, sender);
      }

      // CACHE_MODE_LOCAL avoids handling by StateTransferInterceptor and any potential locks in StateTransferLock
      //TODO This must be addressed again. SKIP_LOCKING is just a workaround for issue https://issues.jboss.org/browse/ISPN-2408
      EnumSet<Flag> flags = EnumSet.of(CACHE_MODE_LOCAL, SKIP_LOCKING, IGNORE_RETURN_VALUES, SKIP_SHARED_CACHE_STORE, SKIP_OWNERSHIP_CHECK, SKIP_XSITE_BACKUP);
      for (InternalCacheEntry e : cacheEntries) {
         InvocationContext ctx = icc.createRemoteInvocationContext(sender);
         try {
            PutKeyValueCommand put = useVersionedPut ?
                  commandsFactory.buildVersionedPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), e.getVersion(), flags)
                  : commandsFactory.buildPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), flags);
            put.setPutIfAbsent(true); //todo [anistor] this still does not solve removal cases. we need tombstones for deleted keys. we need to keep a separate set of deleted keys an use it during apply state
            interceptorChain.invoke(ctx, put);
         } catch (Exception ex) {
            log.problemApplyingStateForKey(ex.getMessage(), e.getKey());
         }
      }
      log.debugf("Finished applying state for segment %d of cache %s", segmentId, cacheName);
   }

   public void applyTransactions(Address sender, int topologyId, Collection<TransactionInfo> transactions) {
      log.debugf("Applying %d transactions for cache %s transferred from node %s", transactions.size(), cacheName, sender);
      if (isTransactional) {
         for (TransactionInfo transactionInfo : transactions) {
            CacheTransaction tx = transactionTable.getLocalTransaction(transactionInfo.getGlobalTransaction());
            if (tx == null) {
               tx = transactionTable.getRemoteTransaction(transactionInfo.getGlobalTransaction());
               if (tx == null) {
                  tx = transactionTable.createRemoteTransaction(transactionInfo.getGlobalTransaction(), transactionInfo.getModifications());
                  ((RemoteTransaction) tx).setMissingLookedUpEntries(true);
               }
            }
            for (Object key : transactionInfo.getLockedKeys()) {
               tx.addBackupLockForKey(key);
            }
         }
      }
   }

   // Must run after the CacheLoaderManager
   @Start(priority = 20)
   public void start() {
      fetchEnabled = configuration.clustering().stateTransfer().fetchInMemoryState() || cacheLoaderManager.isFetchPersistentState();
   }

   @Stop(priority = 20)
   @Override
   public void stop() {
      if (trace) {
         log.tracef("Shutting down StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }

      try {
         synchronized (this) {
            // cancel all inbound transfers
            for (Iterator<List<InboundTransferTask>> it = transfersBySource.values().iterator(); it.hasNext(); ) {
               List<InboundTransferTask> inboundTransfers = it.next();
               it.remove();
               for (InboundTransferTask inboundTransfer : inboundTransfers) {
                  inboundTransfer.cancel();
               }
            }
            transfersBySource.clear();
            transfersBySegment.clear();
         }
      } catch (Throwable t) {
         log.errorf(t, "Failed to stop StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
   }

   @Override
   public void dump() {
      log.trace("StateConsumerImpl.dump " + new HashSet<InboundTransferTask>(transfersBySegment.values()));
   }

   @Override
   public CacheTopology getCacheTopology() {
      return cacheTopology;
   }

   private void findSources(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      for (Integer segmentId : segments) {
         Address source = findSource(segmentId, excludedSources);
         // ignore all segments for which there are no other owners to pull data from.
         // these segments are considered empty (or lost) and do not require a state transfer
         if (source != null) {
            Set<Integer> segmentsFromSource = sources.get(source);
            if (segmentsFromSource == null) {
               segmentsFromSource = new HashSet<Integer>();
               sources.put(source, segmentsFromSource);
            }
            segmentsFromSource.add(segmentId);
         }
      }
   }

   private Address findSource(int segmentId, Set<Address> excludedSources) {
      List<Address> owners = cacheTopology.getReadConsistentHash().locateOwnersForSegment(segmentId);
      if (owners.size() == 1 && owners.get(0).equals(rpcManager.getAddress())) {
         return null;
      }

      for (int i = owners.size() - 1; i >= 0; i--) {   // iterate backwards because we prefer to fetch from newer nodes
         Address o = owners.get(i);
         if (!o.equals(rpcManager.getAddress()) && !excludedSources.contains(o)) {
            return o;
         }
      }
      log.noLiveOwnersFoundForSegment(segmentId, cacheName, owners, excludedSources);
      return null;
   }

   private void requestTransactions(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      if (sources.isEmpty()) {
         findSources(segments, sources, excludedSources);
      }

      boolean seenFailures = false;
      while (true) {
         Map<Address, Future<List<TransactionInfo>>> futures = new HashMap<Address, Future<List<TransactionInfo>>>();
         for (Map.Entry<Address, Set<Integer>> e : sources.entrySet()) {
            Address source = e.getKey();
            futures.put(source, requestTransactions(source, e.getValue(), cacheTopology.getTopologyId()));
         }

         Set<Integer> failedSegments = new HashSet<Integer>();

         for (Map.Entry<Address, Future<List<TransactionInfo>>> e : futures.entrySet()) {
            Address source = e.getKey();
            Future<List<TransactionInfo>> future = e.getValue();
            List<TransactionInfo> transactions = null;
            try {
               transactions = future.get();
            } catch (InterruptedException ex) {
               Thread.currentThread().interrupt();
               return;
            } catch (ExecutionException ex) {
               log.error("Failed to execute transaction retrieval task", ex);
            }

            if (transactions != null) {
               applyTransactions(source, cacheTopology.getTopologyId(), transactions);
            } else {
               // if requesting the transactions failed we need to retry from another source
               excludedSources.add(source);
               failedSegments.addAll(sources.remove(source));
            }
         }

         if (failedSegments.isEmpty()) {
            break;
         }

         // look for other sources for all failed segments
         seenFailures = true;
         sources.clear();
         findSources(failedSegments, sources, excludedSources);
      }

      if (seenFailures) {
         sources.clear();
      }
   }

   private Future<List<TransactionInfo>> requestTransactions(final Address source, final Set<Integer> segments, final int topologyId) {
      return executorService.submit(new Callable<List<TransactionInfo>>() {
         @Override
         public List<TransactionInfo> call() {
            StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(StateRequestCommand.Type.GET_TRANSACTIONS, rpcManager.getAddress(), topologyId, segments);
            Map<Address, Response> responses = rpcManager.invokeRemotely(Collections.singleton(source), cmd, ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout);

            Response response = responses.get(source);
            if (response instanceof SuccessfulResponse) {
               return (List<TransactionInfo>) ((SuccessfulResponse) response).getResponseValue();
            }
            log.failedToRetrieveTransactionsForSegments(segments, cacheName, source);
            return null;
         }
      });
   }

   private void requestSegments(final Set<Integer> segments, final Map<Address, Set<Integer>> sources, final Set<Address> excludedSources) {
      if (sources.isEmpty()) {
         findSources(segments, sources, excludedSources);
      }

      for (Map.Entry<Address, Set<Integer>> e : sources.entrySet()) {
         addTransfer(e.getValue(), e.getKey());
      }

      executorService.submit(new Runnable() {
         @Override
         public void run() {
            while (true) {
               Set<InboundTransferTask> tasks;
               synchronized (StateConsumerImpl.this) {
                  tasks = new HashSet<InboundTransferTask>(transfersBySegment.values());
               }

               for (InboundTransferTask task : tasks) {
                  task.requestSegments();
               }

               Set<Integer> failedSegments = new HashSet<Integer>();

               for (InboundTransferTask task : tasks) {
                  boolean success = false;
                  try {
                     success = task.getSegmentsFuture().get();
                  } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                     break;
                  } catch (ExecutionException e) {
                     log.error("Failed to execute segment retrieval task", e);
                  }

                  if (!success) {
                     if (removeTransfer(task)) {  // remove it as it will be retried from another source
                        // if requesting the segments failed we need to retry from another source
                        excludedSources.add(task.getSource());
                        failedSegments.addAll(task.getSegments());
                     }
                  }
               }

               if (failedSegments.isEmpty()) {
                  break;
               }

               // look for other sources for all failed segments
               sources.clear();
               findSources(failedSegments, sources, excludedSources);

               for (Map.Entry<Address, Set<Integer>> e : sources.entrySet()) {
                  addTransfer(e.getValue(), e.getKey());
               }
            }
         }
      });
   }

   /**
    * Remove the segment's data from the data container and cache store because we no longer own it.
    *
    * @param segments to be cancelled and discarded
    */
   private void discardSegments(Set<Integer> segments) {
      synchronized (this) {
         List<Integer> segmentsToCancel = new ArrayList<Integer>(segments);
         while (!segmentsToCancel.isEmpty()) {
            int segmentId = segmentsToCancel.remove(0);
            InboundTransferTask inboundTransfer = transfersBySegment.remove(segmentId);
            if (inboundTransfer != null) { // we need to check the transfer was not already completed
               log.debugf("Cancelling inbound state transfer for segment %d of cache %s", segmentId, cacheName);
               Set<Integer> cancelledSegments = new HashSet<Integer>(segmentsToCancel);
               cancelledSegments.retainAll(inboundTransfer.getSegments());
               segmentsToCancel.removeAll(cancelledSegments);
               inboundTransfer.cancelSegments(cancelledSegments);   //this will also remove it from transfersBySource if the entire task gets cancelled
            }
         }
      }

      // gather all keys from data container that belong to the segments that are being removed
      Set<Object> keysToRemove = new HashSet<Object>();
      for (InternalCacheEntry ice : dataContainer) {
         Object key = ice.getKey();
         if (segments.contains(getSegment(key))) {
            keysToRemove.add(key);
         }
      }

      // gather all keys from cache store that belong to the segments that are being removed
      CacheStore cacheStore = getCacheStore();
      if (cacheStore != null) {
         //todo [anistor] extend CacheStore interface to be able to specify a filter when loading keys (ie. keys should belong to desired segments)
         try {
            Set<Object> storedKeys = cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer));
            for (Object key : storedKeys) {
               if (segments.contains(getSegment(key))) {
                  keysToRemove.add(key);
               }
            }

         } catch (CacheLoaderException e) {
            log.failedLoadingKeysFromCacheStore(e);
         }
      }

      log.debugf("Removing state for segments %s of cache %s", segments, cacheName);
      if (!keysToRemove.isEmpty()) {
         try {
            InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateFromL1Command(true, EnumSet.of(CACHE_MODE_LOCAL, SKIP_LOCKING), keysToRemove);
            InvocationContext ctx = icc.createNonTxInvocationContext();
            interceptorChain.invoke(ctx, invalidateCmd);

            log.debugf("Invalidated %d keys, data container of cache %s now has %d keys", keysToRemove.size(), cacheName, dataContainer.size());
            if (trace) log.tracef("Invalidated keys: %s", keysToRemove);
         } catch (CacheException e) {
            log.failedToInvalidateKeys(e);
         }
      }

      //todo [anistor] call CacheNotifier.notifyDataRehashed
   }

   private int getSegment(Object key) {
      // there we can use any CH version because the routing table is not involved
      return cacheTopology.getReadConsistentHash().getSegment(key);
   }

   /**
    * Obtains the CacheStore that will be used for purging segments that are no longer owned by this node.
    * The CacheStore will be purged only if it is enabled and it is not shared.
    */
   private CacheStore getCacheStore() {
      if (cacheLoaderManager != null && cacheLoaderManager.isEnabled() && !cacheLoaderManager.isShared()) {
         return cacheLoaderManager.getCacheStore();
      }
      return null;
   }

   private InboundTransferTask addTransfer(Set<Integer> segments, Address source) {
      synchronized (this) {
         segments.removeAll(transfersBySegment.keySet());
         if (!segments.isEmpty()) {
            InboundTransferTask inboundTransfer = new InboundTransferTask(segments, source,
                  cacheTopology.getTopologyId(), this, rpcManager, commandsFactory, executorService, timeout, cacheName);
            for (int segmentId : segments) {
               transfersBySegment.put(segmentId, inboundTransfer);
            }
            List<InboundTransferTask> inboundTransfers = transfersBySource.get(inboundTransfer.getSource());
            if (inboundTransfers == null) {
               inboundTransfers = new ArrayList<InboundTransferTask>();
               transfersBySource.put(inboundTransfer.getSource(), inboundTransfers);
            }
            inboundTransfers.add(inboundTransfer);
            return inboundTransfer;
         } else {
            return null;
         }
      }
   }

   private boolean removeTransfer(InboundTransferTask inboundTransfer) {
      synchronized (this) {
         List<InboundTransferTask> transfers = transfersBySource.get(inboundTransfer.getSource());
         if (transfers != null) {
            if (transfers.remove(inboundTransfer)) {
               if (transfers.isEmpty()) {
                  transfersBySource.remove(inboundTransfer.getSource());
               }
               transfersBySegment.keySet().removeAll(inboundTransfer.getSegments());
               return true;
            }
         }
      }
      return false;
   }

   void onTaskCompletion(InboundTransferTask inboundTransfer) {
      log.tracef("Completion of inbound transfer task: %s ", inboundTransfer);
      removeTransfer(inboundTransfer);

      if (activeTopologyUpdates.get() == 0) {
         notifyEndOfTopologyUpdate(cacheTopology.getTopologyId());
      }
   }
}
