
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author root
 */
public class MySQL {

    private static Connection getConToDB() throws SQLException {
        String dbURL = "jdbc:mysql://localhost:3306/PLAsensor"
                + "?verifyServerCertificate=false"
                + "&useSSL=false"
                + "&requireSSL=false";

        return DriverManager.getConnection(dbURL, "root", "asdf");
    }

    public static int getRoomStatus(int roomID, int dayWeek, int time) {
        int res = -1;
        try {
            Connection con = getConToDB();
            PreparedStatement stmt = con.prepareStatement(
                    "SELECT roomStatus FROM Room WHERE "
                    + " roomId=14"
                    + " and day_week=1"
                    + " and time=180"
            //+ "roomId=" + roomID
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

    public static HashMap<String, Integer> getActivesStatusPerRoom(int roomID, int dayWeek, int time) {
        HashMap<String, Integer> res = new HashMap<>();
        try {
            Connection con = getConToDB();
            PreparedStatement stmt = con.prepareStatement(
                    "SELECT activeID, activeStatus FROM Active WHERE "
                    + " roomId=14"
                    + " and day_week= 1"
                    + " and time=180"
            //+ "roomId=" + roomID
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

    public static void addRoomsStatus(HashMap<Integer, Integer> roomIDStatus, int dayWeek, int time) {
        try {
            Connection con = getConToDB();

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

    public static void addActivesStatus(HashMap<String, Integer> activeIDStatus,
            HashMap<String, Integer> activeIDLocation, int dayWeek, int time) {
        try {
            Connection con = getConToDB();

            String query = "INSERT INTO Active(activeId, activeStatus, roomID, day_week, time) VALUES ";

            for (Map.Entry<String, Integer> entry : activeIDStatus.entrySet()) {
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
