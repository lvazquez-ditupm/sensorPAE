
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class acts as DAO for the MySQL database
 *
 * @author UPM (member of DHARMA Development Team) (http://dharma.inf.um.es)
 * @version 1.0
 */
public class MySQL {

    /**
     * Establece una conexión con la base de datos
     *
     * @param user usuario
     * @param password contraseña
     */
    private static Connection getConToDB(String user, String password) throws SQLException {
        String dbURL = "jdbc:mysql://localhost:3306/PLAsensor"
                + "?verifyServerCertificate=false"
                + "&useSSL=false"
                + "&requireSSL=false";

        return DriverManager.getConnection(dbURL, user, password);
    }

    /**
     * Recupera el estado de la sala en un momento dado
     *
     * @param user usuario DB
     * @param passwd contraseña DB
     * @param roomID identificador de la sala
     * @param dayWeek día de la semana
     * @param time segundos desde medianoche
     * @return estado de la sala (1/0)
     */
    public static int getRoomStatus(String user, String passwd, int roomID, int dayWeek, int time) {
        int res = -1;
        try {
            Connection con = getConToDB(user, passwd);
            PreparedStatement stmt = con.prepareStatement(
                    "SELECT roomStatus FROM Room WHERE "
                    + "roomId=" + roomID
                    + " and day_week=3"
                    + " and time=45000"
            //+ " and day_week=" + dayWeek
            //+ " and time=" + time
            );
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                res = rs.getInt("roomStatus");
                break;
            }

            con.close();

        } catch (SQLException sqle) {
            System.out.println("Error en la ejecución:"
                    + sqle.getErrorCode() + " " + sqle.getMessage());
        }
        return res;
    }

    /**
     * Recupera el estado de los activos de una sala
     *
     * @param user usuario DB
     * @param passwd contraseña DB
     * @param roomID identificador de la sala
     * @param dayWeek día de la semana
     * @param time segundos desde medianoche
     * @return mapa con los activos y sus estados (0-3)
     */
    public static HashMap<String, Integer> getActivesStatusPerRoom(String user, String passwd, int roomID, int dayWeek, int time) {
        HashMap<String, Integer> res = new HashMap<>();
        try {
            Connection con = getConToDB(user, passwd);
            PreparedStatement stmt = con.prepareStatement(
                    "SELECT activeID, activeStatus FROM Active WHERE "
                    + "roomId=" + roomID
                    + " and day_week= 3"
                    + " and time=45000"
            //+ " and day_week=" + dayWeek
            //+ " and time=" + time
            );
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                res.put(rs.getString("activeId"), rs.getInt("activeStatus"));
            }

            con.close();

        } catch (SQLException sqle) {
            System.out.println("Error en la ejecución:"
                    + sqle.getErrorCode() + " " + sqle.getMessage());
        }
        return res;
    }

    /**
     * Registra en la base de datos los estados de las salas
     *
     * @param user usuario DB
     * @param passwd contraseña DB
     * @param roomIDStatus mapa con las salas y sus estados (0/1)
     * @param dayWeek día de la semana
     * @param time segundos desde medianoche
     */
    public static void addRoomsStatus(String user, String passwd, HashMap<Integer, Integer> roomIDStatus, int dayWeek, int time) {
        try {
            Connection con = getConToDB(user, passwd);

            String query = "INSERT INTO Room(roomId, day_week, time, roomStatus) VALUES ";

            for (Map.Entry<Integer, Integer> entry : roomIDStatus.entrySet()) {
                query += "(" + entry.getKey() + "," + dayWeek + "," + time + ","
                        + entry.getValue() + "),";
            }

            query = query.substring(0, query.length() - 1) + ";";

            PreparedStatement stmt = con.prepareStatement(query);
            stmt.executeUpdate();

            con.close();

        } catch (SQLException sqle) {
            System.out.println("Error en la ejecución:"
                    + sqle.getErrorCode() + " " + sqle.getMessage());
        }
    }

    /**
     * Registra en la base de datos los estados y localizaciones de los activos
     * @param user usuario DB
     * @param passwd contraseña DB
     * @param activeIDStatus mapa con los activos y sus estados (0-3)
     * @param activeIDLocation mapa con los activos y las salas en las que están
     * @param dayWeek día de la semana
     * @param time segundos desde medianoche
     */
    public static void addActivesStatus(String user, String passwd, HashMap<String, Integer> activeIDStatus,
            HashMap<String, Integer> activeIDLocation, int dayWeek, int time) throws Exception {
        try {
            Connection con = getConToDB(user, passwd);

            String query = "INSERT INTO Active(activeId, activeStatus, roomID, day_week, time) VALUES ";

            for (Map.Entry<String, Integer> entry : activeIDStatus.entrySet()) {
                if (entry.getValue() < 0 || entry.getValue() > 3) {
                    throw new Exception("Estado en activo " + entry.getKey() + " no válido");
                }
                query += "(\"" + entry.getKey() + "\"," + entry.getValue() + ","
                        + activeIDLocation.get(entry.getKey()) + "," + dayWeek + "," + time + "),";
            }

            query = query.substring(0, query.length() - 1) + ";";

            PreparedStatement stmt = con.prepareStatement(query);
            stmt.executeUpdate();

            con.close();

        } catch (SQLException sqle) {
            System.out.println("Error en la ejecución:"
                    + sqle.getErrorCode() + " " + sqle.getMessage());
        }
    }

}
