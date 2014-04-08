package com.hmsonline.storm.es.trident;


import org.elasticsearch.action.bulk.BulkResponse;

import java.io.Serializable;

public interface ExceptionHandler extends Serializable {

//    void onElasticSearchException(ElasticsearchException e);
    void onElasticSearchException(Exception e);

    void onBulkRequestFailure(BulkResponse response);
}
