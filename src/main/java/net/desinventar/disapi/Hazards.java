package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import org.springframework.stereotype.Component;

/* Datacards class to get the full values for DesInventar's data.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.0
 * */
@Component
//@Service
public class Hazards {
	private final String ctycode;
	private HashMap<String, Object> events = new HashMap<>();
	
/* Empty constructor (not used)
*/	public Hazards() {
		this.ctycode = "";
	}

/* Main constructor:
 * @param code - Country codification
 * @param rs - database resultset 
 */
	public Hazards(String code, ResultSet rs) throws SQLException {
		this.ctycode = code;
        do {
			HashMap<String, Object> row = new HashMap<>();
			row.put("name", rs.getString("name"));
			row.put("description", rs.getString("desc"));
			row.put("causes", rs.getString("causes"));
			this.events.put(rs.getString("event"), row);
		} while (rs.next());
	}
	
	public String getCtycode() {
		return this.ctycode;
	}

	public HashMap<String, Object> getHazards() {
    	return this.events;
	}

}
