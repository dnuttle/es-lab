package com.adobe.iridium.es;

import org.apache.log4j.Logger;

public class ESDriver {

  private static final Logger LOG = Logger.getLogger(ESDriver.class);
  
  public static void main(String[] args) {
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    String index = args[2];
    ESUtil util = new ESUtil(host, port);
    String mappingFile = System.getProperty("user.dir") + "/src/main/resources/template.json";
    String feedFile = System.getProperty("user.dir") + "/src/main/resources/feed.json";
    try {
      util.dropIndex(index); //drops if already exists
      util.createIndex(index);
      util.createAlias(index, index + "-alias");
      util.createMappings(index, mappingFile);
      util.indexJsonFeed(index, feedFile, false /*delete*/);
    } catch (Exception e) {
      LOG.error(e);
    }
  }

}
