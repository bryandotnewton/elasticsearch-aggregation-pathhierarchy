package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;
import java.util.List;

@FunctionalInterface
interface DateHierarchyAggregatorSupplier {
    Aggregator build(String name,
            AggregatorFactories factories,
            AggregationContext context,
            ValuesSource.Numeric valuesSource,
            BucketOrder order,
            long minDocCount,
            DateHierarchyAggregator.BucketCountThresholds bucketCountThresholds,
            List<DateHierarchyAggregationBuilder.RoundingInfo> roundingsInfo,
            Aggregator parent,
            Map<String, Object> metaData) throws IOException;
}
