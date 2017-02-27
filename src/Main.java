
/**
 *
 * @author root
 */
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        FileReader f = null;
        try {
            byte[] encoded = Files.readAllBytes(Paths.get("/home/saturno/NetBeansProjects/SensorPAE/test.json"));
            //System.out.println(detectAnomalies(new String(encoded, "UTF-8")));
            addDataToDB(new String(encoded, "UTF-8"));

        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);

        }
    }

    private static ArrayList<String> detectAnomalies(String path) throws SQLException {

        ArrayList<LinkedTreeMap<String, Object>> jsonMap = new Gson().fromJson(path, ArrayList.class);
        ArrayList<String> anomalies = new ArrayList<>();

        Date now = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(now);
        int weekDay = calendar.get(Calendar.DAY_OF_WEEK);

        Calendar fromMidnight = Calendar.getInstance();
        fromMidnight.set(Calendar.HOUR, 0);
        fromMidnight.set(Calendar.MINUTE, 0);
        fromMidnight.set(Calendar.SECOND, 0);
        fromMidnight.set(Calendar.MILLISECOND, 0);

        long secondsFromMidnight = (calendar.getTimeInMillis() - fromMidnight.getTimeInMillis()) / 1000;
        int time = (int) (900 * (Math.ceil(Math.abs(secondsFromMidnight / 900))));

        for (LinkedTreeMap<String, Object> roomActive : jsonMap) {
            int roomID = (int) (double) roomActive.get("SALA");
            int roomStatus = (int) (double) roomActive.get("ESTADO");
            LinkedTreeMap<String, Double> activesStatus = new LinkedTreeMap<>();
            try {
                ArrayList<LinkedTreeMap<String, Double>> activesList = (ArrayList<LinkedTreeMap<String, Double>>) roomActive.get("ACTIVOS");
                for (LinkedTreeMap<String, Double> active : activesList) {
                    activesStatus.putAll(active);
                }
            } catch (ClassCastException e) {
                activesStatus.putAll((LinkedTreeMap<String, Double>) roomActive.get("ACTIVOS"));
            }

            int roomStatusDB = MySQL.getRoomStatus(roomID, weekDay, time);
            HashMap<String, Integer> activesStatusDB = MySQL.getActivesStatusPerRoom(roomID, weekDay, time);

            if (roomStatusDB == -1 || activesStatusDB.isEmpty()) {
                throw new SQLException("No se han encontrado datos en la base de datos");
            }

            if (roomStatus != roomStatusDB) {
                anomalies.add("La sala \"" + roomID + "\" tiene un ocupación anómala: " + roomStatus);
            }

            int diff = activesStatusDB.size() - activesStatus.size();

            if (diff > 0) {
                for (String active : activesStatusDB.keySet()) {
                    if (!activesStatus.containsKey(active)) {
                        anomalies.add("Activo \"" + active + "\" no encontrado en la sala " + roomID);
                    } else if (activesStatus.get(active).intValue() != activesStatusDB.get(active)) {
                        anomalies.add("Activo \"" + active + "\" con valor anómalo");
                    }
                }
            } else if (diff < 0) {
                for (String active : activesStatus.keySet()) {
                    if (!activesStatusDB.containsKey(active)) {
                        anomalies.add("Activo \"" + active + "\" no presente en la base de datos");
                    } else if (activesStatus.get(active).intValue() != activesStatusDB.get(active)) {
                        anomalies.add("Activo \"" + active + "\" con valor anómalo");
                    }
                }
            } else {
                for (String active : activesStatus.keySet()) {
                    if (!activesStatusDB.containsKey(active)) {
                        anomalies.add("Activo \"" + active + "\" no presente en la base de datos");
                    } else if (activesStatus.get(active).intValue() != activesStatusDB.get(active)) {
                        anomalies.add("Activo \"" + active + "\" con valor anómalo: " + activesStatus.get(active).intValue());
                    }
                }
            }
        }

        return anomalies;
    }

    private static void addDataToDB(String path) throws SQLException {
        ArrayList<LinkedTreeMap<String, Object>> jsonMap = new Gson().fromJson(path, ArrayList.class);
        HashMap<Integer, Integer> roomData = new HashMap<>();
        HashMap<String, Integer> activeStatus = new HashMap<>();
        HashMap<String, Integer> activeLocation = new HashMap<>();
        LinkedTreeMap<String, Double> activesStatus = new LinkedTreeMap<>();

        Date now = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(now);
        int weekDay = calendar.get(Calendar.DAY_OF_WEEK);

        Calendar fromMidnight = Calendar.getInstance();
        fromMidnight.set(Calendar.HOUR, 0);
        fromMidnight.set(Calendar.MINUTE, 0);
        fromMidnight.set(Calendar.SECOND, 0);
        fromMidnight.set(Calendar.MILLISECOND, 0);

        long secondsFromMidnight = (calendar.getTimeInMillis() - fromMidnight.getTimeInMillis()) / 1000;
        int time = (int) (900 * (Math.ceil(Math.abs(secondsFromMidnight / 900))));

        for (LinkedTreeMap<String, Object> roomActive : jsonMap) {
            int roomID = (int) (double) roomActive.get("SALA");
            int roomStatus = (int) (double) roomActive.get("ESTADO");
            try {
                ArrayList<LinkedTreeMap<String, Double>> activesList = (ArrayList<LinkedTreeMap<String, Double>>) roomActive.get("ACTIVOS");
                for (LinkedTreeMap<String, Double> active : activesList) {
                    activesStatus.putAll(active);
                }
            } catch (ClassCastException e) {
                activesStatus.putAll((LinkedTreeMap<String, Double>) roomActive.get("ACTIVOS"));
            }
            roomData.put(roomID, roomStatus);
            for (String active : activesStatus.keySet()) {
                activeStatus.put(active, activesStatus.get(active).intValue());
                activeLocation.put(active, roomID);
            }
            activesStatus.clear();
        }
        MySQL.addRoomsStatus(roomData, weekDay, time);
        MySQL.addActivesStatus(activeStatus, activeLocation, time, time);
    }
}
