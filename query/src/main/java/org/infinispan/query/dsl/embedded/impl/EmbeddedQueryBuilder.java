package org.infinispan.query.dsl.embedded.impl;

import java.util.Map;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
final class EmbeddedQueryBuilder extends BaseQueryBuilder {

   private final QueryEngine<?> queryEngine;

   EmbeddedQueryBuilder(EmbeddedQueryFactory queryFactory, QueryEngine queryEngine, String rootType) {
      super(queryFactory, rootType);
      this.queryEngine = queryEngine;
   }

   @Override
   protected Query makeQuery(String queryString, Map<String, Object> namedParameters) {
      return new DelegatingQuery<>(queryEngine, queryFactory, queryString, namedParameters, getProjectionPaths(), startOffset, maxResults);
   }
}
