package com.hmsonline.storm.es.trident;

import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingExceptionHandler implements ExceptionHandler {
    public static final Logger LOG = LoggerFactory.getLogger(LoggingExceptionHandler.class);

    @Override
    public void onElasticSearchException(Exception e) {
        LOG.error("Unexpected exception during index operation", e);
    }

    @Override
    public void onBulkRequestFailure(BulkResponse response) {
        LOG.error("Bulk request failed: {}", response.buildFailureMessage());
    }
}
