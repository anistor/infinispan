package org.infinispan.server.test.cs;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * test on ae3f0facc89196f90485d01c9f08c4716b038654
 *
 * @author Wolf-Dieter Fink
 */
public class SimpleStringKeyedDbCacheDemo {
   private static final DateFormat dFormat = new SimpleDateFormat("hh:mm:ss.SSS");
   private static final String initialPrompt = "Choose action:\n" + "============= \n" + "add  -  add a property\n" + "g   -  get a property\n"
         + "rm  -  remove a property\n" + "keys   -  print all property names\n" + "size-  print cache size" + "q   -  quit\n";

   private BufferedReader con;
   private RemoteCacheManager cacheManager;
   private RemoteCache<String, Object> cache;

   static {
      Logger.getLogger("").setLevel(Level.FINEST);
   }

   public SimpleStringKeyedDbCacheDemo(BufferedReader con) {
      this.con = con;
      Properties p = new Properties();
      p.put("infinispan.client.hotrod.server_list", "localhost:11222");
      //p.put("infinispan.client.hotrod.tcp_no_delay", "false");
      p.put("infinispan.client.hotrod.socket_timeout", "5000");
      p.put("infinispan.client.hotrod.max_retries", "0");
//    	p.put("infinispan.client.hotrod.connect_timeout", "500");
      cacheManager = new RemoteCacheManager(p);
      cache = cacheManager.getCache("string");
   }

   private String con_readLine(String s) {
      System.out.println(s);
      try {
         return con.readLine();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   public void addPropertyWithRead() throws IOException {
      System.out.println("Property key: ");
      String key = con.readLine();
      if (key == null) {
         // nothing
      } else if (cache.containsKey(key)) {
         System.out.println("Property exists!");
      } else {
         System.out.println("Property value: ");
         String pValue = con.readLine();

         cache.put(key, pValue);
         System.out.println("Put successful KEY hash=" + key.hashCode());
      }
   }

   public void addProperty() {
      String key = con_readLine("Property key : ");
      if (key == null) {
         // nothing
      } else {
         String pValue = con_readLine("Property value: ");

         cache.put(key, pValue);
         System.out.println("Put successful KEY hash=" + key.hashCode());
      }
   }

   public void addPropertyWithLifespan() {
      String key = con_readLine("Property key : ");
      if (key == null) {
         // nothing
      } else if (cache.containsKey(key)) {
         System.out.println("Property exists!");
      } else {
         String pValue = con_readLine("Property value: ");

         cache.put(key, pValue, 10L, TimeUnit.SECONDS);
         System.out.println("Put successful KEY hash=" + key.hashCode() );
      }
   }

   public void removeProperty() {
      String key = con_readLine("Property key: ");
      if (key == null) {
         // nothing
      } else {
         String x = (String) cache.get(key);
         if (x != null) {
            cache.remove(key);
         } else {
            System.out.println("Property not found!");
         }
      }
   }

   public void showProperty() {
      String key = con_readLine("Property key: ");
      if (key == null) {
         System.out.println("no key given");
      } else {
         long start = System.currentTimeMillis();
         String x = (String) cache.get(key);
         if (x != null) {
            System.out.println("Value = " + x);
         } else {
            System.out.println("Property not found!");
         }
         System.out.println(showTimeAndDuration(start));
      }
   }

   public void addData() {
      long key = Long.parseLong(con_readLine("Start Key (number): "));
      long count = Long.parseLong(con_readLine("Number of entries: "));
      for (int i = 0; i < count; i++) {
         String cKey = "" + (key + i);
         cache.put(cKey, "Test Data Key=" + cKey);

         if (i > 0 && i % 1000 == 0) {
            System.out.println(i + " created");
         }
      }
   }

   public void addBigData() {
      long key = Long.parseLong(con_readLine("Start Key (number): "));
      long count = Long.parseLong(con_readLine("Number of entries: "));

      for (int i = 0; i < count; i++) {
         String cKey = "" + (key + i);
         cache.put(cKey, new DGvalue(cKey, "no expiration"));

         if (i > 0 && i % 1000 == 0) {
            System.out.println(i + " created");
         }
      }
   }

   public void addDataWithLifespan() {
      long key = Long.parseLong(con_readLine("Start Key (number): "));
      long count = Long.parseLong(con_readLine("Number of entries: "));

      for (int i = 0; i < count; i++) {
         long t = System.currentTimeMillis();
         t += 100000;
         String cKey = "" + (key + i);
         cache.put(cKey, "expire at " + new Date(t), 100, TimeUnit.SECONDS);

         if (i > 0 && i % 1000 == 0) {
            System.out.println(i + " created");
         }
      }
   }

   public void addBigDataWithLifespan() {
      long key = Long.parseLong(con_readLine("Start Key (number): "));
      long count = Long.parseLong(con_readLine("Number of entries: "));

      for (int i = 0; i < count; i++) {
         long t = System.currentTimeMillis();
         t += 20000;
         String cKey = "" + (key + i);
         cache.put(cKey, new DGvalue(cKey, "expire at " + new Date(t)), 30, TimeUnit.SECONDS);

         if (i > 0 && i % 1000 == 0) {
            System.out.println(i + " created");
         }
      }
   }

   private void print() {
      Set<String> keys = cache.keySet();

      for (String key : keys) {
         System.out.println(key + " = " + cache.get(key));
      }
      System.out.println();
   }

   private void printSize() {
      System.out.println("Size of cache = " + cache.size() );
   }

   private void printKeys() {
      System.out.println("Key List = " + cache.keySet().toString());
   }

   private void printKeyHash() {
      for (String key : (Set<String>) cache.keySet()) {
         System.out.println(key + " hash=" + key.hashCode() );
      }
   }

   public void stop() {
      cacheManager.stop();
   }

   private String showTime() {
      return "  (" + dFormat.format(new Date()) + ")";
   }

   private String showTimeAndDuration(long start) {
      return "  took: " + (System.currentTimeMillis() - start) + "ms (" + dFormat.format(new Date()) + ")";
   }

   public static void main(String[] args) throws Exception {

      BufferedReader con = new BufferedReader(new InputStreamReader(System.in));
      SimpleStringKeyedDbCacheDemo manager = new SimpleStringKeyedDbCacheDemo(con);
      System.out.printf(initialPrompt);

      while (true) {
         System.out.println(">");
         String action = con.readLine();
         try {
            if ("add".equals(action)) {
               manager.addPropertyWithRead();
            } else if ("addNoCheck".equals(action)) {
               manager.addProperty();
            } else if ("addT".equals(action)) {
               manager.addPropertyWithLifespan();
            } else if ("rm".equals(action)) {
               manager.removeProperty();
            } else if ("g".equals(action)) {
               manager.showProperty();
            } else if ("p".equals(action)) {
               manager.print();
            } else if ("size".equals(action)) {
               manager.printSize();
            } else if ("keys".equals(action)) {
               manager.printKeys();
            } else if ("addData".equals(action)) {
               manager.addData();
            } else if ("addDataT".equals(action)) {
               manager.addDataWithLifespan();
            } else if ("addBData".equals(action)) {
               manager.addBigData();
            } else if ("addBDataT".equals(action)) {
               manager.addBigDataWithLifespan();
            } else if ("keyhash".equals(action)) {
               manager.printKeyHash();
            } else if ("q".equals(action)) {
               manager.stop();
               break;
            }
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }


   public static class DGvalue implements Serializable {
      private final String key;
      private final String info;
      private final String data;

      public DGvalue(String key, String info) {
         this.key = key;
         this.info = info;

         this.data = createLongString();
      }

      public String toString() {
         return "Key=" + key + " " + info + "  dataSize=" + data.length();
      }

   }

   private static String createLongString() {
      StringBuilder x = new StringBuilder();

      for (int i = 0; i < 5000; i++) {
         x.append("1234567890");
      }

      return x.toString();
   }
}
