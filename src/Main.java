
/**
 * This class executes the "Presencia+Activos+Estado" (PAE) sensor
 *
 * @author UPM (member of DHARMA Development Team) (http://dharma.inf.um.es)
 * @version 1.0
 */
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    public static void main(String[] args) {
        final int T = Integer.parseInt(args[1]);
        FileReader f = null;
        Calendar midnight = Calendar.getInstance();
        midnight.set(Calendar.HOUR_OF_DAY, 0);
        midnight.set(Calendar.MINUTE, 0);
        midnight.set(Calendar.SECOND, 0);
        midnight.set(Calendar.MILLISECOND, 0);

        Calendar nowCal = Calendar.getInstance();
        long secondsFromMidnight = 0;

        if (args.length != 6 || (!args[0].equals("exec") && !args[0].equals("train"))) {
            System.err.println("No se ha introducido un parámentro correcto (exec/train)");
            System.exit(0);
        }

        // Espera hasta que el tiempo actual acabe en 0
        while (true) {
            nowCal.setTime(new Date());
            secondsFromMidnight = (nowCal.getTimeInMillis() - midnight.getTimeInMillis()) / 1000;

            if (secondsFromMidnight % 10 == 0) {
                break;
            }
        }

        System.out.println("-- Start PAE sensor --");

        int weekDay;
        int time;

        while (true) {
            nowCal.setTime(new Date());
            secondsFromMidnight = (nowCal.getTimeInMillis() - midnight.getTimeInMillis()) / 1000;

            weekDay = nowCal.get(Calendar.DAY_OF_WEEK);
            time = (int) (T * (Math.ceil(Math.abs(secondsFromMidnight / T)))); //Redondea a múltiplo de T (segundos)

            try {
                byte[] encoded = Files.readAllBytes(Paths.get(args[4]));
                if (args[0].equals("exec")) {
                    System.out.println("Detectando anomalías:");
                    ArrayList<String> anomalies = detectAnomalies(args[2], args[3], new String(encoded, "UTF-8"), weekDay, time);
                    processAnomalies(anomalies, secondsFromMidnight, args[5]);
                } else if (args[0].equals("train")) {
                    addDataToDB(args[2], args[3], new String(encoded, "UTF-8"), weekDay, time);
                } else {
                    throw new Exception("Parámetro incorrecto (exec/train)");
                }
                Thread.sleep(T*1000);
            } catch (Exception ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(0);
            }
        }
    }

    /**
     * Obtiene los valores de estado de las salas y activos, y los compara con
     * la base de datos para detectar anomalías
     *
     * @param input contenido del fichero input
     * @param weekDay día de la semana (1-domingo, 7-sábado)
     * @param time segundos desde la medianoche
     * @return lista con las anomalías detectadas
     */
    private static ArrayList<String> detectAnomalies(String user, String passwd, String input, int weekDay, int time) throws SQLException {

        ArrayList<LinkedTreeMap<String, Object>> jsonMap = new Gson().fromJson(input, ArrayList.class);
        ArrayList<String> anomalies = new ArrayList<>();

        for (LinkedTreeMap<String, Object> roomActive : jsonMap) {
            int roomID = (int) (double) roomActive.get("SALA");
            int roomStatus = (int) (double) roomActive.get("ESTADO");
            LinkedTreeMap<String, Double> activesStatus = new LinkedTreeMap<>();
            activesStatus.putAll((LinkedTreeMap<String, Double>) roomActive.get("ACTIVOS"));

            int roomStatusDB = MySQL.getRoomStatus(user, passwd, roomID, weekDay, time);
            HashMap<String, Integer> activesStatusDB = MySQL.getActivesStatusPerRoom(user, passwd, roomID, weekDay, time);

            if (roomStatusDB == -1 || activesStatusDB.isEmpty()) {
                throw new SQLException("No se han encontrado datos en la base de datos");
            }

            if (roomStatus != roomStatusDB) {
                anomalies.add("La sala \"" + roomID + "\" tiene una ocupación anómala: " + roomStatus);
            }

            int diff = activesStatusDB.size() - activesStatus.size();

            if (diff > 0) {
                for (String active : activesStatusDB.keySet()) {
                    if (!activesStatus.containsKey(active)) {
                        anomalies.add("Activo \"" + active + "\" no encontrado en la sala " + roomID);
                    } else if (!activesStatusDB.containsKey(active)) {
                        anomalies.add("Activo \"" + active + "\" detectado en la sala " + roomID);

                    } else if (activesStatus.get(active).intValue() != activesStatusDB.get(active)) {
                        anomalies.add("Activo \"" + active + "\" con valor anómalo: "
                                + activesStatus.get(active).intValue() + "/" + activesStatusDB.get(active));
                    }
                }
            } else {
                for (String active : activesStatus.keySet()) {
                    if (!activesStatus.containsKey(active)) {
                        anomalies.add("Activo \"" + active + "\" no encontrado en la sala " + roomID);
                    } else if (!activesStatusDB.containsKey(active)) {
                        anomalies.add("Activo \"" + active + "\" detectado en la sala " + roomID);

                    } else if (activesStatus.get(active).intValue() != activesStatusDB.get(active)) {
                        anomalies.add("Activo \"" + active + "\" con valor anómalo: "
                                + activesStatus.get(active).intValue() + "/" + activesStatusDB.get(active));
                    }
                }
            }

            activesStatus.clear();
        }

        return anomalies;
    }

    /**
     * Añade los valores de estado de salas y activos a la base de datos
     *
     * @param path ruta al fichero input
     * @param weekDay día de la semana (1-domingo, 7-sábado)
     * @param time segundos desde la medianoche
     */
    private static void addDataToDB(String user, String passwd, String path, int weekDay, int time) throws Exception {
        ArrayList<LinkedTreeMap<String, Object>> jsonMap = new Gson().fromJson(path, ArrayList.class);
        HashMap<Integer, Integer> roomData = new HashMap<>();
        HashMap<String, Integer> activeStatus = new HashMap<>();
        HashMap<String, Integer> activeLocation = new HashMap<>();
        LinkedTreeMap<String, Double> activesStatus = new LinkedTreeMap<>();

        for (LinkedTreeMap<String, Object> roomActive : jsonMap) {
            int roomID = (int) (double) roomActive.get("SALA");
            int roomStatus = (int) (double) roomActive.get("ESTADO");
            try {
                ArrayList<LinkedTreeMap<String, Double>> activesList
                        = (ArrayList<LinkedTreeMap<String, Double>>) roomActive.get("ACTIVOS");
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
        MySQL.addRoomsStatus(user, passwd, roomData, weekDay, time);
        MySQL.addActivesStatus(user, passwd, activeStatus, activeLocation, weekDay, time);
        System.out.println("Añadidos datos a la base de datos");
    }

    /**
     * Clasifica las anomalías detectadas
     *
     * @param anomalies lista de anomalías
     * @param secondsFromMidnight segundos desde la medianoche
     * @param path ruta al fichero output
     */
    private static void processAnomalies(ArrayList<String> anomalies, long secondsFromMidnight, String path) {

        Gson gson = new Gson();
        HashMap<String, Object> json = new HashMap<>();
        HashMap<String, String> roomOcupationAnomalies = new HashMap<>();
        HashMap<String, String[]> activeUsageAnomalies = new HashMap<>();
        HashMap<String, String[]> activeRegistrationAnomalies = new HashMap<>();

        HashMap<String, String> isInRoom = new HashMap<>();
        HashMap<String, String> isNotInRoom = new HashMap<>();

        for (String anomaly : anomalies) {

            System.out.println(anomaly);

            if (anomaly.contains("tiene una ocupación anómala")) {
                anomaly = anomaly.substring(anomaly.indexOf("\"") + 1);
                String room = anomaly.substring(0, anomaly.indexOf("\""));
                String anomalyStatus = anomaly.substring(anomaly.length() - 1);
                roomOcupationAnomalies.put(room, anomalyStatus);

            } else if (anomaly.contains("con valor anómalo")) {
                anomaly = anomaly.substring(anomaly.indexOf("\"") + 1);
                String active = anomaly.substring(0, anomaly.indexOf("\""));
                String anomalyStatusStr = anomaly.substring(anomaly.length() - 3);
                String[] anomalyStatus = anomalyStatusStr.split("/");
                activeUsageAnomalies.put(active, anomalyStatus);

            } else if (anomaly.contains("detectado en la sala")) {
                anomaly = anomaly.substring(anomaly.indexOf("\"") + 1);
                String active = anomaly.substring(0, anomaly.indexOf("\""));
                String room = anomaly.substring(anomaly.indexOf("sala ") + 5);
                isInRoom.put(active, room);

            } else if (anomaly.contains("no encontrado en la sala")) {
                anomaly = anomaly.substring(anomaly.indexOf("\"") + 1);
                String active = anomaly.substring(0, anomaly.indexOf("\""));
                String room = anomaly.substring(anomaly.indexOf("sala ") + 5);
                isNotInRoom.put(active, room);
            }

            if (!isInRoom.isEmpty() && !isNotInRoom.isEmpty()) {
                ArrayList<String> delete = new ArrayList<>();
                for (String active : isInRoom.keySet()) {
                    if (isNotInRoom.containsKey(active)) {
                        String[] movement = new String[2];
                        movement[0] = isNotInRoom.get(active);
                        movement[1] = isInRoom.get(active);
                        activeRegistrationAnomalies.put(active, movement);
                        delete.add(active);
                    }
                }
                for (String deleteItem : delete) {
                    isNotInRoom.remove(deleteItem);
                    isInRoom.remove(deleteItem);
                }
                delete.clear();
            }
        }

        json.put("OcupationAnom", (HashMap<String, Object>) (Object) roomOcupationAnomalies);
        json.put("UsageAnom", (HashMap<String, Object>) (Object) activeUsageAnomalies);
        json.put("RegistrationAnom", (HashMap<String, Object>) (Object) activeRegistrationAnomalies);
        json.put("Time", secondsFromMidnight);

        if (roomOcupationAnomalies.isEmpty() && activeRegistrationAnomalies.isEmpty()
                && activeRegistrationAnomalies.isEmpty()) {
            return;
        }

        String jsonString = gson.toJson(json);

        PrintWriter writer;
        try {
            writer = new PrintWriter(path, "UTF-8");
            writer.println(jsonString);
            writer.close();

        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
