package telran.monitoring;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;

import telran.monitoring.api.SensorData;
import telran.monitoring.logging.Logger;
import telran.monitoring.logging.LoggerStandard;

public class App {
    Logger logger = new LoggerStandard("jump-pulse-recognizer");
    public void handleRequest(final DynamodbEvent event, final Context context) {
      event.getRecords().forEach(r -> {
       Map<String, AttributeValue>map =  r.getDynamodb().getNewImage();
       
       long patientId = Long.parseLong(map.get("patientId").getN());
       int value = Integer.parseInt(map.get("value").getN());
       long timestamp = Long.parseLong(map.get("timestamp").getN());
           SensorData sensorData = new SensorData(patientId, value, timestamp);
           logger.log("finest", sensorData.toString());
      }); 

      
    }
}
