/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.testng.Assert.*;

/**
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.DataRehashedEventTest")
@CleanupAfterMethod
public class DataRehashedEventTest extends MultipleCacheManagersTest {

   private ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   private DataRehashedListener rehashListener;

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(1, defaultConfig);

      Cache<Object, Object> c1 = cache(0, "cache");
      rehashListener = new DataRehashedListener();
      c1.addListener(rehashListener);
   }

   public void testJoinAndLeave() {
      ConsistentHash preCH = advancedCache(0, "cache").getDistributionManager().getReadConsistentHash();
      assertEquals(rehashListener.removeEvents().size(), 0);

      addClusterEnabledCacheManager(defaultConfig);
      cache(1, "cache");
      ConsistentHash postCH = advancedCache(0, "cache").getDistributionManager().getReadConsistentHash();

      List<DataRehashedEvent<Object, Object>> newEvents = rehashListener.removeEvents();
      DataRehashedEvent<Object, Object> pre = newEvents.get(0);
      DataRehashedEvent<Object, Object> post = newEvents.get(1);

      assertTrue(pre.isPre());
      assertEquals(pre.getConsistentHashAtStart(), preCH);
      assertEquals(pre.getMembersAtStart(), preCH.getMembers());
      // we could get this "intermediate" CH with TopologyChanged events, but this should be enough
      assertNotNull(pre.getConsistentHashAtEnd());
      assertEquals(pre.getMembersAtEnd(), postCH.getMembers());

      assertFalse(post.isPre());
      assertEquals(post.getConsistentHashAtStart(), preCH);
      assertEquals(post.getMembersAtStart(), preCH.getMembers());
      assertEquals(post.getConsistentHashAtEnd(), postCH);
      assertEquals(post.getMembersAtEnd(), postCH.getMembers());
   }

   @Listener
   public static class DataRehashedListener {
      private volatile List<DataRehashedEvent<Object, Object>> events = new CopyOnWriteArrayList<DataRehashedEvent<Object, Object>>();

      @DataRehashed
      public void onDataRehashed(DataRehashedEvent<Object, Object> e) {
         events.add(e);
      }

      List<DataRehashedEvent<Object, Object>> removeEvents() {
         List<DataRehashedEvent<Object, Object>> oldEvents = events;
         events = new CopyOnWriteArrayList<DataRehashedEvent<Object, Object>>();
         return oldEvents;
      }
   }
}
