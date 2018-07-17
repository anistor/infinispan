package org.infinispan.client.hotrod.impl.query;

import java.time.Instant;
import java.util.Date;
import java.util.Map;

import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQueryBuilder;
import org.infinispan.query.dsl.impl.QueryStringCreator;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class RemoteQueryBuilder extends BaseQueryBuilder {

   private final RemoteCacheImpl<?, ?> cache;

   private final SerializationContext serializationContext;

   RemoteQueryBuilder(RemoteQueryFactory queryFactory, RemoteCacheImpl<?, ?> cache, SerializationContext serializationContext, String rootType) {
      super(queryFactory, rootType);
      this.cache = cache;
      this.serializationContext = serializationContext;
   }

   @Override
   protected Query makeQuery(String queryString, Map<String, Object> namedParameters) {
      return new RemoteQuery(queryFactory, cache, serializationContext, queryString, namedParameters, getProjectionPaths(), startOffset, maxResults);
   }

   @Override
   protected QueryStringCreator makeQueryStringCreator() {
      if (serializationContext == null) {
         return super.makeQueryStringCreator();
      }
      return new RemoteQueryStringCreator();
   }

   //TODO [anistor] these overrides are only used for remote query with Lucene engine
   private final class RemoteQueryStringCreator extends QueryStringCreator {

      @Override
      protected <E extends Enum<E>> String renderEnum(E argument) {
         EnumMarshaller<E> encoder = (EnumMarshaller<E>) serializationContext.getMarshaller(argument.getClass());
         return String.valueOf(encoder.encode(argument));
      }

      @Override
      protected String renderDate(Date argument) {
         return Long.toString(argument.getTime());
      }

      @Override
      protected String renderInstant(Instant argument) {
         return Long.toString(argument.toEpochMilli());
      }
   }
}
