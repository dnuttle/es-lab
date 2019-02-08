package com.adobe.iridium.es;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ESDriver2 {

  private static final Logger LOG = LoggerFactory.getLogger(ESDriver2.class);
  private ObjectMapper mapper;
  private int count;
  private ESUtil2 util;
  
  public ESDriver2() {
    mapper = new ObjectMapper();
    util = new ESUtil2();
  }
  
  public static void main(String[] args) throws Exception {
    ESUtil2 util = new ESUtil2();
    RestHighLevelClient client = util.getClient();
    ESDriver2 driver = new ESDriver2();
    //driver.index(client);
    String feedFile = System.getProperty("user.dir") + "\\src\\main\\resources\\feed.json";
    String mappingFile = System.getProperty("user.dir") + "\\src\\main\\resources\\mapping.json";
//    driver.indexFile(client, "doc", "test", feedFile);
    driver.putMapping("test", "doc", mappingFile);
    client.close();
  }
  
  private void search(RestHighLevelClient client, String index, String type) {
    
  }
  
  private void putMapping(String index, String type, String mappingFile) 
  throws IOException {
    /*
    PutMappingRequest req = new PutMappingRequest(index);
    req.type(type);
    JsonNode root = mapper.readTree(new File(mappingFile));
    req.source(mapper.writeValueAsBytes(root), XContentType.JSON);
    */
    
    StringEntity se = new StringEntity("{\"abc\":123}", ContentType.APPLICATION_JSON);
    LOG.debug(se.toString());
    LOG.debug(se.getContent().toString());
    RestClient client = util.getLowLevelClient();
    Response resp = client.performRequest("GET", "/" + index + "/_search");
    String json = EntityUtils.toString(resp.getEntity(), "UTF-8");
    LOG.debug(json);
    JsonNode root = mapper.readTree(json);
    LOG.debug("Hits: " + root.get("hits").get("total").asInt());
  }
  
  private void index(RestHighLevelClient client) throws IOException {
    IndexRequest req = new IndexRequest(
        "test",
        "doc",
        "1"
    );
    String json = "{\"user\":\"dan\"}";
    req.source(json, XContentType.JSON);
    IndexResponse resp = client.index(req);
    String verb = "unknown";
    if (resp.getResult() == DocWriteResponse.Result.CREATED) {
      verb = "created";
    } else if (resp.getResult() == DocWriteResponse.Result.UPDATED) {
      verb = "updated";
    }
    
    LOG.debug("Indexed doc id " + resp.getId() + " of type " + resp.getType() + " " + verb + " in index " + resp.getIndex());
    ReplicationResponse.ShardInfo shardInfo = resp.getShardInfo();
    if (shardInfo.getFailed() > 0) {
      LOG.warn("Indexing failed on " + shardInfo.getFailed() + " shards");
    }
  }

  private void indexFile(RestHighLevelClient client, String index, String type, String file) throws JsonProcessingException, IOException {
    int count = 0;
    JsonNode root = mapper.readTree(new File(file));
    Iterator<JsonNode> iter = root.iterator();
    BulkRequest req = new BulkRequest();
    while (iter.hasNext()) {
      JsonNode node = iter.next();
      String id = node.get("id").asText();
      IndexRequest ireq = new IndexRequest(index, "doc", id);
      ireq.source(mapper.writeValueAsBytes(node), XContentType.JSON);
      req.add(ireq);
      count++;
      if (count == 1000) {
        indexBulkRequest(client, req);
        count = 0;
        req = new BulkRequest();
      }
    }
    if (req.numberOfActions() > 0) {
      indexBulkRequest(client, req);
    }
  }
  
  private void indexBulkRequest(RestHighLevelClient client, BulkRequest req) throws IOException {
    int total = req.numberOfActions();
    int successful = 0;
    int updated = 0;
    int created = 0;
    int deleted = 0;
    BulkResponse resp = client.bulk(req);
    for (BulkItemResponse item : resp.getItems()) {
      if (!item.isFailed()) {
        successful++;
        if (item.getOpType() == OpType.UPDATE) {
          updated++;
        }
        if (item.getOpType() == OpType.INDEX || item.getOpType() == OpType.CREATE) {
          created++;
        }
        if (item.getOpType() == OpType.DELETE) {
          deleted++;
        }
      } else {
        LOG.debug(item.getFailureMessage());
      }
    }
    String msg = String.format("Processed %d records, %d created, %d updated, %d deleted and %d failures", 
        total, created, updated, deleted, total - successful);
    LOG.debug(msg);
  }
  
}
