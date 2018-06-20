package com.blokaly.sylvia;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StockTwitsRestClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(StockTwitsRestClient.class);
  private static final String API_HOST = "https://api.stocktwits.com/api/2";
  private static final String STREAMS_PATH = "/streams";
  private static final String SYMBOL_PATH = API_HOST + STREAMS_PATH + "/symbol/%s.json";
  private static final String SYMBOL_MAX_PATH = SYMBOL_PATH + "?max=%s";
  private final Gson gson = new Gson();
  private final JsonParser parser = new JsonParser();

  public String retrieveMessages(String symbol, long max) {

    String url;
    if (max == 0) {
      url = String.format(SYMBOL_PATH, symbol.toUpperCase());
    } else if (max < 0) {
      return null;
    } else {
      url = String.format(SYMBOL_MAX_PATH, symbol.toUpperCase(), max);
    }
    LOGGER.info("Requesting stocktwits: {}", url);
    try (HttpReader reader = new HttpReader((HttpURLConnection)new URL(url).openConnection())) {
      HttpURLConnection conn = reader.getConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Content-type", "application/x-www-form-urlencoded");
      conn.setRequestProperty("Accept", "application/json");
      return reader.read();
    } catch (Exception ex) {
      LOGGER.error("Error requesting stocktwits messages: " + url, ex);
      return null;
    }
  }

  private long update(ElasticClient elasticClient, String symbol, long max) {
    try {
      String json = retrieveMessages(symbol, max);
      if (json != null) {
        JsonObject res = parser.parse(json).getAsJsonObject();
        int status = res.get("response").getAsJsonObject().get("status").getAsInt();
        if (status == 200) {
          JsonObject cursor = res.get("cursor").getAsJsonObject();
          LOGGER.info("Stocktwits messages retrieved successfully: {}", gson.toJson(cursor));
          JsonArray messages = res.get("messages").getAsJsonArray();
          int updated = elasticClient.upsert(messages);
          LOGGER.info("Stocktwits updated [{}]", updated);
          if (updated > 0) {
            return nextMax(cursor);
          }
        } else {
          LOGGER.error("Stocktwits messages retrieved failed: {}", json);
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Error update", ex);
    }
    return -1;
  }

  private Runnable newUpdateTask(ScheduledExecutorService executorService, ElasticClient elasticClient, String symbol, long max) {
    return () -> {
      long next = update(elasticClient, symbol, max);
      if (next > 0) {
        LOGGER.info("next task in 10 secs...");
        executorService.schedule(newUpdateTask(executorService, elasticClient, symbol, next), 10, TimeUnit.SECONDS);
      } else {
        executorService.shutdown();
      }
    };
  }

  private static long nextMax(JsonObject cursor) {
    boolean hasMore = cursor.get("more").getAsBoolean();
    if (hasMore) {
      long last = cursor.get("max").getAsLong();
      return last + 1;
    } else {
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {

    final ElasticClient elasticClient = new ElasticClient();
    final StockTwitsRestClient client = new StockTwitsRestClient();

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    executorService.submit(client.newUpdateTask(executorService, elasticClient, "btc.x", 0));

    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
    elasticClient.stop();
  }
}
