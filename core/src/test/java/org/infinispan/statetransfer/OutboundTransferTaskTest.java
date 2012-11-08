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
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.*;

/**
 * Tests OutboundTransferTask.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.OutboundTransferTaskTest")
public class OutboundTransferTaskTest {

   private static final Log log = LogFactory.getLog(OutboundTransferTaskTest.class);

   public void testSending() throws Exception {
      ExecutorService mockExecutorService = mock(ExecutorService.class);
      when(mockExecutorService.submit(any(Runnable.class))).thenAnswer(new Answer<Future<?>>() {
         @Override
         public Future<?> answer(InvocationOnMock invocation) {
            Runnable runnable = (Runnable) invocation.getArguments()[0];
            runnable.run();
            return null;
         }
      });

      Cache cache = mock(Cache.class);
      when(cache.getName()).thenReturn("testCache");

      RpcManager rpcManager = mock(RpcManager.class);
      when(rpcManager.getAddress()).thenReturn(new TestAddress(0));

      CommandsFactory commandsFactory = mock(CommandsFactory.class);
      when(commandsFactory.buildStateResponseCommand(any(Address.class), anyInt(), any(Collection.class))).thenAnswer(new Answer<StateResponseCommand>() {
         @Override
         public StateResponseCommand answer(InvocationOnMock invocation) {
            return new StateResponseCommand("testCache", (Address) invocation.getArguments()[0],
                  ((Integer) invocation.getArguments()[1]).intValue(),
                  (Collection<StateChunk>) invocation.getArguments()[2]);
         }
      });

      TransactionTable transactionTable = mock(TransactionTable.class);
      when(transactionTable.getLocalTransactions()).thenReturn(Collections.<LocalTransaction>emptyList());
      when(transactionTable.getRemoteTransactions()).thenReturn(Collections.<RemoteTransaction>emptyList());

      // create list of 6 members
      List<Address> members = new ArrayList<Address>();
      for (int i = 0; i < 6; i++) {
         members.add(new TestAddress(i));
      }

      // create CH
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      ConsistentHash ch = chf.create(new MurmurHash3(), 2, 4, members);

      final List<InternalCacheEntry> cacheEntries = new ArrayList<InternalCacheEntry>();
      cacheEntries.add(new ImmortalCacheEntry(new TestKey("key1", 0, ch), "value1"));
      cacheEntries.add(new ImmortalCacheEntry(new TestKey("key2", 0, ch), "value2"));

      DataContainer dataContainer = mock(DataContainer.class);
      when(dataContainer.iterator()).thenAnswer(new Answer<Iterator<InternalCacheEntry>>() {
         @Override
         public Iterator<InternalCacheEntry> answer(InvocationOnMock invocation) {
            return cacheEntries.iterator();
         }
      });

      StateProviderImpl stateProvider = mock(StateProviderImpl.class);

      OutboundTransferTask task = new OutboundTransferTask(new TestAddress(5), Collections.singleton(0), 1000, 1, ch,
            stateProvider, dataContainer, null, rpcManager, commandsFactory, 10000, "testCache");

      task.execute(mockExecutorService);

      InOrder inOrder = inOrder(rpcManager, stateProvider);
      inOrder.verify(rpcManager).getAddress();
      ArgumentCaptor<StateResponseCommand> argumentCaptor = ArgumentCaptor.forClass(StateResponseCommand.class);
      inOrder.verify(rpcManager).invokeRemotely(any(Collection.class), argumentCaptor.capture(), any(ResponseMode.class), anyLong(), anyBoolean());

      Object[] cmdParams = argumentCaptor.getValue().getParameters();
      assertEquals(new TestAddress(0), cmdParams[0]);
      assertEquals(1, cmdParams[1]);
      List<StateChunk> chunks = (List<StateChunk>) cmdParams[2];
      assertEquals(1, chunks.size());
      assertEquals(0, chunks.get(0).getSegmentId());
      assertEquals(2, chunks.get(0).getCacheEntries().size());
      assertTrue(chunks.get(0).isLastChunk());

      inOrder.verify(stateProvider).onTaskCompletion(task);

      verifyNoMoreInteractions(stateProvider, rpcManager);
   }

   public void testCancellationWithInterrupt() throws Exception {
      testCancellation(false);
   }

   public void testCancellationWithInterruptedException() throws Exception {
      testCancellation(true);
   }

   private void testCancellation(final boolean throwInterruptedException) throws Exception {
      ThreadFactory threadFactory = new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r);
         }
      };

      ExecutorService pooledExecutorService = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingDeque<Runnable>(), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

      Cache cache = mock(Cache.class);
      when(cache.getName()).thenReturn("testCache");

      RpcManager rpcManager = mock(RpcManager.class);
      when(rpcManager.getAddress()).thenReturn(new TestAddress(0));

      CommandsFactory commandsFactory = mock(CommandsFactory.class);
      CacheLoaderManager cacheLoaderManager = mock(CacheLoaderManager.class);

      TransactionTable transactionTable = mock(TransactionTable.class);
      when(transactionTable.getLocalTransactions()).thenReturn(Collections.<LocalTransaction>emptyList());
      when(transactionTable.getRemoteTransactions()).thenReturn(Collections.<RemoteTransaction>emptyList());

      // create list of 6 members
      List<Address> members = new ArrayList<Address>();
      for (int i = 0; i < 6; i++) {
         members.add(new TestAddress(i));
      }

      // create CH
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      ConsistentHash ch = chf.create(new MurmurHash3(), 2, 4, members);

      final List<InternalCacheEntry> cacheEntries = new ArrayList<InternalCacheEntry>();
      cacheEntries.add(new ImmortalCacheEntry(new TestKey("key1", 0, ch), "value1"));
      cacheEntries.add(new ImmortalCacheEntry(new TestKey("key2", 0, ch), "value2"));

      DataContainer dataContainer = mock(DataContainer.class);
      when(dataContainer.iterator()).thenAnswer(new Answer<Iterator<InternalCacheEntry>>() {
         @Override
         public Iterator<InternalCacheEntry> answer(InvocationOnMock invocation) {
            return cacheEntries.iterator();
         }
      });

      StateProviderImpl stateProvider = mock(StateProviderImpl.class);

      final CountDownLatch invokeRemotelyLatch = new CountDownLatch(1);
      final CountDownLatch taskCompletionLatch = new CountDownLatch(2);
      final AtomicBoolean wasInterrupted = new AtomicBoolean(false);

      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            try {
               invokeRemotelyLatch.countDown();

               final Object lock = new Object();
               synchronized (lock) { // this lock is never notified
                  try {
                     lock.wait(); // wait until interrupted
                  } catch (InterruptedException e) {
                     wasInterrupted.set(true);
                     if (throwInterruptedException) {
                        throw e;
                     } else {
                        Thread.currentThread().interrupt();
                     }
                  }
               }
               return null;
            } finally {
               taskCompletionLatch.countDown();
            }
         }
      }).when(rpcManager).invokeRemotely(any(Collection.class), any(StateResponseCommand.class), any(ResponseMode.class), anyLong(), anyBoolean());

      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) {
            taskCompletionLatch.countDown();
            return null;
         }
      }).when(stateProvider).onTaskCompletion(any(OutboundTransferTask.class));

      final OutboundTransferTask task = new OutboundTransferTask(new TestAddress(5), Collections.singleton(0), 1, 1, ch,
            stateProvider, dataContainer, cacheLoaderManager, rpcManager, commandsFactory, 10000, "testCache");

      task.execute(pooledExecutorService);

      assertFalse(task.isCancelled());

      invokeRemotelyLatch.await();

      verify(rpcManager).getAddress();
      verify(rpcManager).invokeRemotely(any(Collection.class), any(StateResponseCommand.class), any(ResponseMode.class), anyLong(), anyBoolean());

      task.cancel();

      taskCompletionLatch.await();

      assertTrue(wasInterrupted.get());
      assertTrue(task.isCancelled());

      verify(stateProvider).onTaskCompletion(task);
      verifyNoMoreInteractions(stateProvider, rpcManager);
   }
}
