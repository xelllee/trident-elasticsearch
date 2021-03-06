package com.hmsonline.storm.es.trident;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;


public class NodeClientFactory implements ClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NodeClientFactory.class);
    @Override
    public Client makeClient(Map conf) {
        String clusterName = (String)conf.get(CLUSTER_NAME);
        LOG.info("Attaching node client to cluster: '{}'", clusterName);
        //http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-node.html
        Node node = nodeBuilder().clusterName(clusterName).client(true).data(false).node();
        return node.client();
    }
}
