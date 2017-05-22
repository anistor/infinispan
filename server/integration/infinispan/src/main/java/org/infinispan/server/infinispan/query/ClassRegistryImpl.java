package org.infinispan.server.infinispan.query;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@Scope(Scopes.GLOBAL)
public class ClassRegistryImpl implements ClassRegistry {

   private Map<String, Class<?>> classes = new ConcurrentHashMap<>();

   @Override
   public Map<String, Class<?>> getClasses() {
      return classes;
   }
}
