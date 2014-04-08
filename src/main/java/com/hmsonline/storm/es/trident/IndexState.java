package com.hmsonline.storm.es.trident;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.List;

public class IndexState implements State {

    private Client client;
    private ExceptionHandler exceptionHandler;
    private long count = 0;
    private long currentTxid;
    private long batchCount = 0;

    public static final Logger LOG = LoggerFactory.getLogger(IndexState.class);


    public IndexState(Client client, ExceptionHandler exceptionHandler) {
        this.client = client;
        this.exceptionHandler = exceptionHandler;
    }


    @Override
    public void beginCommit(Long aLong) {
        currentTxid = aLong;
        batchCount++;
    }

    @Override
    public void commit(Long aLong) {

    }

    public void updateState(List<TridentTuple> tridentTuples, IndexTupleMapper mapper, TridentCollector tridentCollector) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (TridentTuple tuple : tridentTuples) {
            if (!mapper.delete(tuple)) {
                String parentId = mapper.toParentId(tuple);
                if (StringUtils.isEmpty(parentId)) {
                    bulkRequest.add(client.prepareIndex(
                            mapper.toIndexName(tuple),
                            mapper.toTypeName(tuple),
                            mapper.toId(tuple)
                    ).setSource(mapper.toDocument(tuple)));
                } else {
                    bulkRequest.add(client.prepareIndex(
                            mapper.toIndexName(tuple),
                            mapper.toTypeName(tuple),
                            mapper.toId(tuple)
                    ).setSource(mapper.toDocument(tuple)).setParent(parentId));
                }
            } else {
                bulkRequest.add(client.prepareDelete(
                        mapper.toIndexName(tuple),
                        mapper.toTypeName(tuple),
                        mapper.toId(tuple)
                ));
            }
        }

        //update count
        count += tridentTuples.size();

        try {
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                this.exceptionHandler.onBulkRequestFailure(bulkResponse);
            }
            if (batchCount % 100 == 0) {
                LOG.info(count + " rows haven sent to ES in " + batchCount + " batches current: txId is " + currentTxid);
            }

        } catch (Exception e) {
            this.exceptionHandler.onElasticSearchException(e);
        }
    }
}
