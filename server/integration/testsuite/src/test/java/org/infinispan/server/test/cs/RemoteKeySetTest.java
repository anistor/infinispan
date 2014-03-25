package org.infinispan.server.test.cs;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for keySet() method on a replicated remote cache that uses a JdbcStringBasedStore. See
 * https://issues.jboss.org/browse/ISPN-4125
 *
 * @author anistor@redhat.com
 */
@RunWith(Arquillian.class)
@WithRunningServer("remote-cache-keySet-with-string-keyed-jdbc-store")
public class RemoteKeySetTest {

   private static final int NUM_KEYS = 2000;

   @InfinispanResource("remote-cache-keySet-with-string-keyed-jdbc-store")
   private RemoteInfinispanServer server;

   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, Integer> remoteCache;
   private RemoteCacheManagerFactory rcmFactory;

   @Before
   public void setUp() throws Exception {
      rcmFactory = new RemoteCacheManagerFactory();
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
            .host(server.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server.getHotrodEndpoint().getPort())
            .marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = rcmFactory.createManager(clientBuilder);
      remoteCache = remoteCacheManager.getCache("testcache");
   }

   @After
   public void tearDown() {
      if (remoteCache != null) {
         remoteCache.clear();
      }
      if (rcmFactory != null) {
         rcmFactory.stopManagers();
      }
      rcmFactory = null;
   }

   @Test
   public void testKeySet() throws Exception {
      for (int i = 0; i < NUM_KEYS; i++) {
         remoteCache.put(i, i);
      }

      Set<Integer> keys = remoteCache.keySet();
      assertNotNull(keys);
      assertEquals(NUM_KEYS, keys.size());
   }
}
