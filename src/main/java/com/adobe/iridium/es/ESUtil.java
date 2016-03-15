package com.adobe.iridium.es;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;


public class ESUtil {
  
  private static final Logger LOG = Logger.getLogger(ESUtil.class);

  private String host;
  private int port;
  
  public ESUtil(String host, int port) {
    this.host = host;
    this.port = port;
  }
  
  public Stream<String> getIndexNames() throws UnknownHostException {
    return Arrays.asList(getMetaData().concreteAllIndices()).stream();
  }
  
  public void createIndex(String index) throws UnknownHostException {
    LOG.info("createIndex (" + index + ")");
    if (!isIndexExists(index)) {
      CreateIndexResponse resp = getClient().admin().indices().prepareCreate(index).get();
      LOG.info("Index create ack: " + resp.isAcknowledged());
    }
  }
  
  public void createAlias(String index, String alias) throws UnknownHostException {
    LOG.info("createAlias (" + alias + ")");
    if (!isIndexExists(index)) {
      createIndex(index);
    }
    AdminClient admin = getClient().admin();
    IndicesAliasesResponse resp = admin.indices().prepareAliases().addAlias(index, alias).get();
    LOG.info("Alias creation request ack: " + resp.isAcknowledged());
  }
  
  public void dropIndex(String index) throws UnknownHostException {
    Client client = getClient();
    AdminClient admin = client.admin();
    if (isIndexExists(index)) {
      DeleteIndexResponse resp = admin.indices().prepareDelete(index).get();
      LOG.info("Acknowledged: " + resp.isAcknowledged());
    } else {
      LOG.info("Index " + index + " does not exist, so not deleted");
    }
  }
  
  public boolean isIndexExists(String index) throws UnknownHostException {
    return getMetaData().index(index) != null;
  }

  /**
   * @param index is name of index to update
   * @param feed is filename with source data
   * @param delete is false if adding/updating records, true if deleting records
   * @throws JsonProcessingException
   * @throws IOException
   */
  public void indexJsonFeed(String index, String feed, boolean delete)
    throws JsonProcessingException, IOException {
    Client client = getClient();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(new File(feed));
    Iterator<JsonNode> iter = root.elements();
    BulkRequestBuilder bulkBuilder = client.prepareBulk();
    int count = 0;
    int batch = 0;
    BulkResponse bresp;
    while (iter.hasNext()) {
      JsonNode doc = iter.next();
      if (!delete) {
        byte[] json = mapper.writeValueAsBytes(doc);
        IndexRequest ireq = client.prepareIndex(index, "doc", doc.get("id").asText())
            .setSource(json).request();
        bulkBuilder.add(ireq);
        
      } else {
        DeleteRequest dreq = client.prepareDelete(index, "doc", doc.get("id").asText()).request();
        bulkBuilder.add(dreq);
      }
      count++;
      batch++;
      if (batch == 1000) {
        batch = 0;
        bresp = bulkBuilder.execute().actionGet();
        for (BulkItemResponse bir : bresp.getItems()) {
          if (bir.isFailed()) {
            LOG.info(bir.getFailure().getMessage());
          }
        }
        bulkBuilder = client.prepareBulk();
        LOG.info("Processed batch");
      }
    }
    if (batch > 0) {
      bresp = bulkBuilder.execute().actionGet();
    }
    LOG.info("Processed " + count);
  }
  
  
  public void createMappings(String index, String mappingFile) throws UnknownHostException,
  JsonProcessingException, IOException {
  InputStream is = new FileInputStream(new File(mappingFile));
  XContentParser parser = XContentFactory.xContent(XContentType.JSON)
      .createParser(is);
    boolean ack = getClient()
      .admin()
      .indices()
      .preparePutMapping(index)
      .setType("doc")
      .setSource(parser.mapOrdered())
      .execute().actionGet().isAcknowledged();
    LOG.info("Mapping ack: " + ack);
  }

  public MetaData getMetaData() throws UnknownHostException {
    return getClient().admin().cluster().prepareState().execute().actionGet().getState().getMetaData();
  }

  public Client getClient() throws UnknownHostException {
    TransportClient client = TransportClient.builder().build()
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
    return client;
  }


}
