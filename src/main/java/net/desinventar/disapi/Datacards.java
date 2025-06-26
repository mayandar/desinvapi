package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import org.springframework.stereotype.Component;

/* Datacards class to get the full values for DesInventar's data.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 0.9
 * */
@Component
//@Service
public class Datacards {
	private final String ctycode;
	private Integer records;
	private String nextPage;
	private HashMap<String, Object> datacards = new HashMap<>();
	
/* Empty constructor (not used)
*/	public Datacards() {
		this.ctycode = "";
		this.records = 0;
		this.nextPage = "nonext";
	}


/* Main constructor:
 * @param code - Country codification
 * @param pg - Page
 * @param rs - database resultset 
 */
	public Datacards(String code, String pg, ResultSet rs) throws SQLException {
		this.ctycode = code;
        int total = 0;
		ResultSetMetaData rsmd = rs.getMetaData();
        int colCount = rsmd.getColumnCount();
		do {
			HashMap<String, Object> fichas = new HashMap<>();
	        for (int i = 1; i <= colCount; i++) {
	        	String colName = rsmd.getColumnName(i);
	        	String colType = rsmd.getColumnTypeName(i);
	        	if (colName.length() > 4 && (colName.substring(0, 4).equals("eff_") || colName.substring(0, 4).equals("geo_") || 
	        		colName.substring(0, 4).equals("rec_") || colName.substring(0, 4).equals("hzd_"))) {
	        		if (colType.equals("int4")) {
	        			// Qualitative effects set True when value reported is -1, other case False
	        			if (colName.equals("eff_withdeaths") || colName.equals("eff_withinjured") || colName.equals("eff_withmissing") ||
	        				colName.equals("eff_withindirecaffected") || colName.equals("eff_withdirectaffected") ||
	        				colName.equals("eff_withdstdwelling") || colName.equals("eff_withdmgdwelling") || 
	        				colName.equals("eff_withothers") || colName.equals("eff_withevacuated") || colName.equals("eff_withrelocated")) {
	        				if (rs.getInt(colName) == -1)
	        					fichas.put(colName, true);
	        				else
	        					fichas.put(colName, false);
	        			}
	        			else
	        				fichas.put(colName, rs.getInt(colName));
	        		}
	        		else if (colType.equals("float4") || colType.equals("float8")) {
	        			fichas.put(colName, rs.getFloat(colName));
	        		}
	        		else {
	        			fichas.put(colName, rs.getString(colName));
	        		}
	        	}
	        	else if (!(colName.equals("clave") || colName.equals("clave_ext") || 
	        			colName.equals("uuid") || colName.equals("total_datacards"))) {
	        		if (colType.equals("int4")) {
	        			fichas.put("ext_" + colName, rs.getInt(colName));
	        		}
	        		else if (colType.equals("float4") || colType.equals("float8")) {
	        			fichas.put("ext_" + colName, rs.getFloat(colName));
	        		}
	        		else {
	        			fichas.put("ext_" + colName, rs.getString(colName));
	        		}
	        	}
	        	else if (colName.equals("total_datacards") && total == 0) {
	        		total = rs.getInt(colName);
	        	}
	        }
			// Add to datacards List
			this.datacards.put(rs.getString("uuid"), fichas);
		} while (rs.next());
		this.records = total;
		int page = Integer.parseInt(pg);
		int totalPages = total / 1000;
		if (page < totalPages)
			this.nextPage = Integer.toString(page+1);
		else
			this.nextPage = "nonext";
	}
	
	public String getCtycode() {
		return this.ctycode;
	}
	public Integer getRecords() {
		return this.records;
	}
	public String getNextPage() {
		return this.nextPage;
	}
    public HashMap<String, Object> getDatacards() {
    	return this.datacards;
    }
}
