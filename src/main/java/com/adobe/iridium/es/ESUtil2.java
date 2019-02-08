package com.adobe.iridium.es;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ESUtil2 {

  public static final String HOST = "localhost";
  public static final int PORT = 9200;
  public static final String SCHEME = "http";
  
  public RestHighLevelClient getClient() {
    return new RestHighLevelClient(getRestClientBuilder());
  }
  
  public RestClient getLowLevelClient() {
    return getRestClientBuilder().build();
  }
  
  public RestClientBuilder getRestClientBuilder() {
    return RestClient.builder(new HttpHost(HOST, PORT, SCHEME));
  }
}
