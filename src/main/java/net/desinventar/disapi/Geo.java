package net.desinventar.disapi;

import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import org.springframework.stereotype.Component;

/* Datacards class to get the full values for DesInventar's data.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.0
 * */
@Component
//@Service
public class Geo {
	private final String ctycode;
	private HashMap<String, Object> geo = new HashMap<>();
	
/* Empty constructor (not used)
*/	public Geo() {
		this.ctycode = "";
	}

/* Main constructor:
 * @param code - Country codification
 * @param rs - database resultset 
 */
	public Geo(String code, ResultSet rs) throws SQLException {
		this.ctycode = code;
		boolean vis0=true, vis1=true, vis2=true;
		HashMap<String, Object> le0 = new HashMap<>();
		HashMap<String, Object> le1 = new HashMap<>();
		HashMap<String, Object> le2 = new HashMap<>();
		HashMap<String, Object> it0 = new HashMap<>();
		HashMap<String, Object> it1 = new HashMap<>();
		HashMap<String, Object> it2 = new HashMap<>();
		do {
			// Create Item Geo
			HashMap<String, String> cod = new HashMap<>();
			cod.put("parent", rs.getString("parent"));
			cod.put("label", rs.getString("label"));
			cod.put("name", rs.getString("name"));
			if (rs.getInt("lev") == 0) {
				if (vis0) {
					le0.put("filename", rs.getString("filename"));
					le0.put("col_code", rs.getString("lev_code"));
					le0.put("col_name", rs.getString("lev_name"));
					vis0 = false;
				}
				it0.put(rs.getString("code"), cod);
			}
			else if (rs.getInt("lev") == 1) {
				if (vis1) {
					le1.put("filename", rs.getString("filename"));
					le1.put("col_code", rs.getString("lev_code"));
					le1.put("col_name", rs.getString("lev_name"));
					vis1 = false;
				}
				it1.put(rs.getString("code"), cod);
			}
			else if (rs.getInt("lev") == 2) {
				if (vis2) {
					le2.put("filename", rs.getString("filename"));
					le2.put("col_code", rs.getString("lev_code"));
					le2.put("col_name", rs.getString("lev_name"));
					vis2 = false;
				}
				it2.put(rs.getString("code"), cod);
			}
		} while (rs.next());
		le0.put("items", it0);
		le1.put("items", it1);
		le2.put("items", it2);
		this.geo.put("0", le0);
		this.geo.put("1", le1);
		this.geo.put("2", le2);
	}
	
	public String getCtycode() {
		return this.ctycode;
	}
    public HashMap<String, Object> getGeo() {
    	return this.geo;
    }
}
