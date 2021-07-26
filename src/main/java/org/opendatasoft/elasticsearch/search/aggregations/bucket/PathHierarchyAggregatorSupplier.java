package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Map;

@FunctionalInterface
interface PathHierarchyAggregatorSupplier {
    Aggregator build(String name,
            AggregatorFactories factories,
            AggregationContext context,
            ValuesSource valuesSource,
            BucketOrder order,
            long minDocCount,
            PathHierarchyAggregator.BucketCountThresholds bucketCountThresholds,
            BytesRef separator,
            int minDepth,
            int maxDepth,
            boolean keepBlankPath,
            Aggregator parent,
            Map<String, Object> metadata) throws IOException;
}
