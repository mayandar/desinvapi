package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import org.springframework.stereotype.Component;

/* Consolidated class to get the full values for DesInventar's data.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 0.9
 * */
@Component
//@Service
public class Consolidated {
	private final String ctycode;
	private final String year;
	private final String indicator;
	private HashMap<String, Object> value = new HashMap<>();
	private String source;
	private HashMap<String, Object> hazards = new HashMap<>();
	private HashMap<String, Object> subdivision1 = new HashMap<>();
	private HashMap<String, Object> otherdisaggregation = new HashMap<>();
	
/* Empty constructor (not used)
 */
	public Consolidated() {
		this.ctycode = "";
		this.year = "";
		this.indicator = "";
	}

/* Main constructor:
 * @param code - Country codification
 * @param yr - Year
 * @param indic - Indicator 
 * @param rs - database resultset 
 */
	public Consolidated(String code, String yr, String indic, ResultSet rs) throws SQLException {
		this.ctycode = code;
		this.year = yr;
		this.indicator = indic;
    	this.source = "DesInventar Official database";
    	// Other disaggregations
		HashMap<String, Object> age = new HashMap<>();
		HashMap<String, Object> sex = new HashMap<>();
		HashMap<String, Object> dis = new HashMap<>();
		HashMap<String, Object> inc = new HashMap<>();
		HashMap<String, Object> agr = new HashMap<>();
		do {
			if (rs.getString(2) != null) {
	    		if (indic.equals("a2a") || indic.equals("a3a") || indic.equals("b2") || indic.equals("b3") || 
	        		indic.equals("b3a") || indic.equals("b4") || indic.equals("b4a") || indic.equals("b5")) {
	        		System.out.println(rs.getString(1) + " : "+ rs.getString(2) + "=" + rs.getInt(3));
	        		//System.out.println("OBJ = " + rs.getObject("number"));
	    			// Null values or 0s set to null.
	    			Integer total = null;
	    			if (rs.getObject("number") != null && !rs.wasNull() && rs.getInt(3) != 0) {
	    				total = (Integer) rs.getInt(3);
			        	switch(rs.getString(1)) {
				        	case "general":
				        			this.value.put("total", total);
				        	break;
				        	case "geography":
				            	HashMap<String, Integer> tot1 = new HashMap<>();
				            	tot1.put("total", total);
				            	this.subdivision1.put(rs.getString(2), tot1);
				        	break;
				        	case "hazard":
				            	HashMap<String, Integer> tot2 = new HashMap<>();
				            	tot2.put("total", total);
				            	this.hazards.put(rs.getString(2), tot2);
				        	break;
				        	case "age":
				            	HashMap<String, Integer> tot3 = new HashMap<>();
				            	tot3.put("total", total);
				            	age.put(rs.getString(2), tot3);
				        	break;
				        	case "sex":
				            	HashMap<String, Integer> tot4 = new HashMap<>();
				            	tot4.put("total", total);
				            	sex.put(rs.getString(2), tot4);
				        	break;
				        	case "disability":
				            	HashMap<String, Integer> tot5 = new HashMap<>();
				            	tot5.put("total", total);
				            	dis.put(rs.getString(2), tot5);
				        	break;
				        	case "income":
				            	HashMap<String, Integer> tot6 = new HashMap<>();
				            	tot6.put("total", total);
				            	inc.put(rs.getString(2), tot6);
				        	break;
			        	}
	    			}
	    		} // end indicators A and B
	    		else if (indic.equals("c2c") || indic.equals("c2l") || indic.equals("c2fo") || indic.equals("c2a") || 
	    				indic.equals("c2fi") || indic.equals("c2lb") || indic.equals("c2la") || indic.equals("c3") ||
	    				indic.equals("c4") || indic.equals("c5a") || indic.equals("c5b") || indic.equals("c5c")) {
	        		//System.out.println(rs.getString(1) + " : "+ rs.getString(2) +"="+ rs.getInt(3)+"="+ rs.getInt(4)+"="+ rs.getInt(5)+"="+ rs.getInt(6));
	    			// Null values set to null.
	    			Double total = null;
	    			if (rs.getObject("total") != null && !rs.wasNull() && rs.getDouble(3) != 0)
	    				total = (Double) rs.getDouble(3);
	    			Double damaged = null;
	    			if (rs.getObject("damaged") != null && !rs.wasNull() && rs.getDouble(4) != 0)
	    				damaged = (Double) rs.getDouble(4);
	    			Double destroyed = null;
	    			if (rs.getObject("destroyed") != null && !rs.wasNull() && rs.getDouble(5) != 0)
	    				destroyed = (Double) rs.getDouble(5);
	    			Double economic = null;
	    			if (rs.getObject("economic") != null && !rs.wasNull() && rs.getDouble(6) != 0)
	    				economic = (Double) rs.getDouble(6);
	    			switch(rs.getString(1)) {
			        	case "general":
			            	this.value.put("total", total);
			            	this.value.put("damaged", damaged);
			            	this.value.put("destroyed", destroyed);
			            	this.value.put("economic", economic);
			        	break;
			        	case "geography":
			            	HashMap<String, Double> tot11 = new HashMap<>();
			            	tot11.put("total", total);
			            	tot11.put("damaged", damaged);
			            	tot11.put("destroyed", destroyed);
			            	tot11.put("economic", economic);
			            	this.subdivision1.put(rs.getString(2), tot11);
			        	break;
			        	case "hazard":
			            	HashMap<String, Double> tot12 = new HashMap<>();
			            	tot12.put("total", total);
			            	tot12.put("damaged", damaged);
			            	tot12.put("destroyed", destroyed);
			            	tot12.put("economic", economic);
			            	this.hazards.put(rs.getString(2), tot12);
			        	break;
			        	case "crops": case "livestocks": case "productive": case "infraestructure":
			            	HashMap<String, Double> tot13 = new HashMap<>();
			            	tot13.put("total", total);
			            	tot13.put("damaged", damaged);
			            	tot13.put("destroyed", destroyed);
			            	tot13.put("economic", economic);
			            	agr.put(rs.getString(2), tot13);
			        	break;
		        	}
	    		} // end indicators C
	    		else if (indic.equals("d6") || indic.equals("d7") || indic.equals("d8")) {
	        		//System.out.println(rs.getString(1) + " : "+ rs.getString(2) + "=" + rs.getInt(3));
	    			// Null values or 0s set to null.
	    			Integer total = null;
	    			if (rs.getInt(3) != 0) {
	    				total = (Integer) rs.getInt(3);
			        	switch(rs.getString(1)) {
				        	case "general":
				        			this.value.put("total", total);
				        	break;
				        	case "geography":
				            	HashMap<String, Integer> tot1 = new HashMap<>();
				            	tot1.put("total", total);
				            	this.subdivision1.put(rs.getString(2), tot1);
				        	break;
				        	case "hazard":
				            	HashMap<String, Integer> tot2 = new HashMap<>();
				            	tot2.put("total", total);
				            	this.hazards.put(rs.getString(2), tot2);
				        	break;
			        	}
	    			}
	    		} // end indicators D
			}
        } while(rs.next());
		if (indic.equals("a2a") || indic.equals("a3a") || indic.equals("b2") || indic.equals("b3") || 
			indic.equals("b3a") || indic.equals("b4") || indic.equals("b4a") || indic.equals("b5")) {
			this.otherdisaggregation.put("Age", age);
	    	this.otherdisaggregation.put("Sex", sex);
	    	this.otherdisaggregation.put("Disability", dis);
	    	this.otherdisaggregation.put("Income", inc);
		}
		else if (indic.equals("c2c")) {
	    	this.otherdisaggregation.put("Crops", agr);
		}
		else if (indic.equals("c2l")) {
	    	this.otherdisaggregation.put("Livestocks", agr);
		}
		else if (indic.equals("c3")) {
	    	this.otherdisaggregation.put("Productive Assets", agr);
		}
		else if (indic.equals("c5c")) {
	    	this.otherdisaggregation.put("Infraestructure", agr);
		}
		else if (indic.equals("d6") || indic.equals("d7") || indic.equals("d8")) {
			// D8 disaggregation?
			//this.otherdisaggregation.put("Age", age);
		}
	}
	
	public String getCtycode() {
		return this.ctycode;
	}
	public String getYear() {
		return this.year;
	}
	public String getIndicator() {
		return this.indicator;
	}
    public HashMap<String, Object> getValue() {
    	return this.value;
    }
    public HashMap<String, Object> getHazards() {
    	return this.hazards;
    }
    public HashMap<String, Object> getSubdivision1() {
    	return this.subdivision1;
    }
    public HashMap<String, Object> getOtherdisaggregation() {
    	return this.otherdisaggregation;
    }
	public String getSource() {
		return source;
	}

}
