package com.blokaly.sylvia;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ElasticClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticClient.class);
  private RestHighLevelClient client;

  public ElasticClient () {
    client = new RestHighLevelClient(
        RestClient.builder(
            new HttpHost("localhost", 9200, "http"),
            new HttpHost("localhost", 9201, "http")));
  }

  public void stop() {
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public int upsert(JsonArray msgArray) {

    int updated = 0;
    Gson gson = new Gson();
    Map<String, JsonObject> messages = StreamSupport.stream(msgArray.spliterator(), false)
        .collect(Collectors.toMap(
            elm -> elm.getAsJsonObject().get("id").getAsString(),
            JsonElement::getAsJsonObject));

    MultiGetRequest request = new MultiGetRequest();
    String indexName = "stocktwits";
    String type = "doc";

    for (String messageId : messages.keySet()) {
      request.add(new MultiGetRequest.Item(indexName, type, messageId)
          .fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE));
    }
    try {
      MultiGetResponse response = client.multiGet(request);
      for (MultiGetItemResponse res : response.getResponses()) {
        if (res.getFailure() == null && res.getResponse().isExists()) {
          messages.remove(res.getId());
        }
      }
      LOGGER.debug("Message ids to be updated: {}", Arrays.toString(messages.keySet().toArray()));

      int noOfDocs = 0;
      BulkRequest batchUpdates = new BulkRequest();
      for (Map.Entry<String, JsonObject> message : messages.entrySet()) {
        JsonObject msgObj = message.getValue();
        String text = msgObj.get("body").getAsString();
        String lemmas = TokenUtil.lemmas(text);
        msgObj.addProperty("lemmas", lemmas);
        batchUpdates.add(new IndexRequest(indexName, type, message.getKey())
            .source(gson.toJson(msgObj), XContentType.JSON));
        noOfDocs++;
      }

      if (noOfDocs > 0) {
        BulkResponse bulkResponse = client.bulk(batchUpdates);
        for (BulkItemResponse bulkRes : bulkResponse) {
          DocWriteResponse itemResponse = bulkRes.getResponse();
          DocWriteResponse.Result result = itemResponse.getResult();
          LOGGER.debug("{} - {}", bulkRes.getId(), result);
          if (result.equals(DocWriteResponse.Result.CREATED)) {
            updated++;
          }
        }
      }

      } catch (IOException e) {
      LOGGER.error("Failed to update stocktwits documents", e);
    }

    return updated;
  }
}
