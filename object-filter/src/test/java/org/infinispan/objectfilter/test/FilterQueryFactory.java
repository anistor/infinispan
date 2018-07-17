package org.infinispan.objectfilter.test;

import java.util.List;
import java.util.Map;

import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.BaseQueryFactory;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
final class FilterQueryFactory extends BaseQueryFactory {

   private final SerializationContext serializationContext;

   FilterQueryFactory(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   FilterQueryFactory() {
      this(null);
   }

   @Override
   public Query create(String queryString) {
      return new FilterQuery(queryString, null, null, -1, -1);
   }

   @Override
   public Query create(String queryString, IndexedQueryMode queryMode) {
      return new FilterQuery(queryString, null, null, -1, -1);
   }

   @Override
   public QueryBuilder from(Class<?> entityType) {
      if (serializationContext != null) {
         // we just check that the type is known to be marshallable
         serializationContext.getMarshaller(entityType);
      }
      return new FilterQueryBuilder(entityType.getName());
   }

   @Override
   public QueryBuilder from(String entityType) {
      if (serializationContext != null) {
         // we just check that the type is known to be marshallable
         serializationContext.getMarshaller(entityType);
      }
      return new FilterQueryBuilder(entityType);
   }

   private final class FilterQueryBuilder extends BaseQueryBuilder {

      FilterQueryBuilder(String rootType) {
         super(FilterQueryFactory.this, rootType);
      }

      @Override
      protected Query makeQuery(String queryString, Map<String, Object> namedParameters) {
         return new FilterQuery(queryString, namedParameters, getProjectionPaths(), startOffset, maxResults);
      }
   }

   private final class FilterQuery extends BaseQuery {

      FilterQuery(String queryString, Map<String, Object> namedParameters, String[] projection, long startOffset, int maxResults) {
         super(FilterQueryFactory.this, queryString, namedParameters, projection, startOffset, maxResults);
      }

      // TODO [anistor] need to rethink the dsl Query/QueryBuilder interfaces to accommodate the filtering scenario ...
      @Override
      public <T> List<T> list() {
         throw new UnsupportedOperationException();
      }

      @Override
      public int getResultSize() {
         throw new UnsupportedOperationException();
      }
   }
}
