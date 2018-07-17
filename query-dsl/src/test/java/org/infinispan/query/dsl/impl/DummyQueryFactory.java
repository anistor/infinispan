package org.infinispan.query.dsl.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;

/**
 * A dummy implementation of the abstract methods in BaseQueryFactory/BaseQueryBuilder/BaseQuery. Just for very basic
 * tests.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class DummyQueryFactory extends BaseQueryFactory {

   @Override
   public Query create(String queryString) {
      return new DummyQuery(queryString, null, null, -1, -1);
   }

   @Override
   public Query create(String queryString, IndexedQueryMode queryMode) {
      return new DummyQuery(queryString, null, null, -1, -1);
   }

   @Override
   public DummyQueryBuilder from(Class<?> entityType) {
      return new DummyQueryBuilder(entityType.getName());
   }

   @Override
   public DummyQueryBuilder from(String entityType) {
      return new DummyQueryBuilder(entityType);
   }

   private final class DummyQueryBuilder extends BaseQueryBuilder {

      DummyQueryBuilder(String rootTypeName) {
         super(DummyQueryFactory.this, rootTypeName);
      }

      @Override
      protected Query makeQuery(String queryString, Map<String, Object> namedParameters) {
         return new DummyQuery(queryString, namedParameters, getProjectionPaths(), startOffset, maxResults);
      }
   }

   private final class DummyQuery extends BaseQuery {

      DummyQuery(String queryString, Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults) {
         super(DummyQueryFactory.this, queryString, namedParameters, projection, startOffset, maxResults);
      }

      @Override
      public <T> List<T> list() {
         return Collections.emptyList();
      }

      @Override
      public int getResultSize() {
         return 0;
      }
   }
}
