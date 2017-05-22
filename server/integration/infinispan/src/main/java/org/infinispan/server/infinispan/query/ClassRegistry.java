package org.infinispan.server.infinispan.query;

import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public interface ClassRegistry {

   Map<String, Class<?>> getClasses();
}
