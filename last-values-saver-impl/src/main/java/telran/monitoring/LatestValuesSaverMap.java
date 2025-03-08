package telran.monitoring;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import telran.monitoring.api.LatestValuesSaver;
import telran.monitoring.api.SensorData;

public class LatestValuesSaverMap implements LatestValuesSaver {
    private final HashMap<Long, SensorData> latestValues = new HashMap<>();

    @Override
    public void addValue(SensorData sensorData) {
        if (sensorData != null) {
            latestValues.put(sensorData.patientId(), sensorData); 
        }
    }

    @Override
    public List<SensorData> getAllValues(long patientId) {
        SensorData data = latestValues.get(patientId);
        if (data != null) {
            return List.of(data); 
        }
        return new ArrayList<>(); 
    }

    @Override
    public SensorData getLastValue(long patientId) {
        return latestValues.get(patientId);
    }

    @Override
    public void clearValues(long patientId) {
        latestValues.remove(patientId);
    }

    @Override
    public void clearAndAddValue(long patientId, SensorData sensorData) {
        latestValues.remove(patientId); 
        if (sensorData != null && sensorData.patientId() == patientId) {
            latestValues.put(patientId, sensorData); 
        }
    }
}