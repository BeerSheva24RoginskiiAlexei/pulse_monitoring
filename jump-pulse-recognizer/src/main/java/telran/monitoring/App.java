package telran.monitoring;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;

import telran.monitoring.api.LatestValuesSaver;
import telran.monitoring.api.SensorData;
import telran.monitoring.logging.Logger;
import telran.monitoring.logging.LoggerStandard;

public class App {
  Logger logger = new LoggerStandard("jump-pulse-recognizer");
  private final LatestValuesSaver latestValuesSaver = new LatestValuesSaverMap();
  private final float factor;
  private final DynamoDB dynamoDB;

  public App() {
    String factorEnv = System.getenv("FACTOR");
    this.factor = factorEnv != null ? Float.parseFloat(factorEnv) : 0.5f;
    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    this.dynamoDB = new DynamoDB(client);
  }

  public void handleRequest(final DynamodbEvent event, final Context context) {
    Table jumpTable = dynamoDB.getTable("jump-pulse-values");

    event.getRecords().forEach(r -> {
      Map<String, AttributeValue> map = r.getDynamodb().getNewImage();

      long patientId = Long.parseLong(map.get("patientId").getN());
      int value = Integer.parseInt(map.get("value").getN());
      long timestamp = Long.parseLong(map.get("timestamp").getN());
      SensorData sensorData = new SensorData(patientId, value, timestamp);

      logger.log("finest", sensorData.toString());

      SensorData lastData = latestValuesSaver.getLastValue(patientId);
      if (lastData != null) {
        int lastValue = lastData.value();
        int currentValue = sensorData.value();
        if (isJump(lastValue, currentValue)) {
          logger.log("info", "Jump detected for patientId: " + patientId +
              ", lastValue: " + lastValue + ", currentValue: " + currentValue);
          Item jumpItem = new Item()
              .withPrimaryKey("patientId", patientId, "timestamp", timestamp)
              .withNumber("lastValue", lastValue)
              .withNumber("currentValue", currentValue);
          jumpTable.putItem(jumpItem);
        }
      }
      latestValuesSaver.addValue(sensorData);
    });
  }

  private boolean isJump(int last, int current) {
    return Math.abs(last - current) / (float) last >= factor;
  }
}