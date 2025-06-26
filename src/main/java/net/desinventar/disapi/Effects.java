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
public class Effects {
	private final String ctycode;
	private HashMap<String, Object> effects = new HashMap<>();
	
/* Empty constructor (not used)
*/	public Effects() {
		this.ctycode = "";
	}

/* Private function
 * @param nam - Name
 * @param desc - Description
 * @param len - length
 * @param type - Type.. doh!
 */
	private HashMap<String, Object> getDefEffect2(String nam, String desc, Integer len, String type) {
		HashMap<String, Object> row = new HashMap<>();
		row.put("label", nam);
		row.put("name", nam);
		row.put("description", desc);
		row.put("length", len);
		row.put("fieldtype", type);
		return row;
	}

/* Main constructor:
 * @param code - Country codification
 * @param rs - database resultset 
 */
	public Effects(String code, ResultSet rs) throws SQLException {
		this.ctycode = code;
		this.effects.put("eff_deaths", this.getDefEffect2("Deaths","# of Deaths", 20, "integer"));
		this.effects.put("eff_injured", this.getDefEffect2("Injured","# of Injured", 20, "integer"));
		this.effects.put("eff_missing", this.getDefEffect2("Missing","# of Missing", 20, "integer"));
		this.effects.put("eff_dstdwelling", this.getDefEffect2("DestroyedDwellings","# of Destroyed dwellings", 20, "integer"));
		this.effects.put("eff_dmgdwelling", this.getDefEffect2("AffectedDwellings","# of Damaged dwellings", 20, "integer"));
		this.effects.put("eff_directaffected", this.getDefEffect2("Victims","# of directed affected people", 20, "integer"));
		this.effects.put("eff_indirectaffected", this.getDefEffect2("Affected","# of indirected affected people", 20, "integer"));
		this.effects.put("eff_relocated", this.getDefEffect2("Relocated","# of relocated people", 20, "integer"));
		this.effects.put("eff_evacuated", this.getDefEffect2("Evacuated","# of evacuated people", 20, "integer"));
		this.effects.put("eff_lossesusd", this.getDefEffect2("LossesUSD","economic $ of losses in USD", 40, "float"));
		this.effects.put("eff_losseslcu", this.getDefEffect2("LossesLocal","economic $ of losses in Local Currency", 40, "float"));
		this.effects.put("eff_educationcenters", this.getDefEffect2("EducationCenters","# of Educational Centers affected", 20, "integer"));
		this.effects.put("eff_healthcenters", this.getDefEffect2("Hospitals","# of Health Centers affected", 20, "integer"));
		this.effects.put("eff_crops", this.getDefEffect2("CropsDamages","# of Hectares affected", 20, "integer"));
		this.effects.put("eff_livestock", this.getDefEffect2("Livestock","# of Livestock destroyed", 20, "integer"));
		this.effects.put("eff_roads", this.getDefEffect2("RoadsDamages","# of mts of roads affected", 20, "integer"));
		this.effects.put("eff_others", this.getDefEffect2("Others","Other effects", 500, "text"));
		this.effects.put("eff_withdeaths", this.getDefEffect2("WithDeaths", "Were there deaths", 10, "yes-no"));
		this.effects.put("eff_withinjured", this.getDefEffect2("WithInjured","Were there injured?", 10, "yes-no"));
		this.effects.put("eff_withmissing", this.getDefEffect2("WithMissing","Were there missings?", 10, "yes-no"));
		this.effects.put("eff_withindirecaffected", this.getDefEffect2("WithAffected","Were there Indirect affected?", 10, "yes-no"));
		this.effects.put("eff_withdirectaffected", this.getDefEffect2("WithVictims","Were there direct affected?", 10, "yes-no"));
		this.effects.put("eff_withevacuated", this.getDefEffect2("WithEvacuated","Were there evacuated people?", 10, "yes-no"));
		this.effects.put("eff_withrelocated", this.getDefEffect2("WithRelocated","Were there relocated people?", 10, "yes-no"));
		this.effects.put("eff_withdstdwelling", this.getDefEffect2("WithDestroyedDwellings","Were there destroyed dwellings?", 10, "yes-no"));
		this.effects.put("eff_withdmgdwelling", this.getDefEffect2("WithAffectedDwellings","Were there affected dwellings?", 10, "yes-no"));
		this.effects.put("eff_withothers", this.getDefEffect2("WithOther","Others Sectors affected?", 10, "yes-no"));
		this.effects.put("eff_withrelief_sec", this.getDefEffect2("WithReliefSector","Relief Sector affected?", 10, "yes-no"));
		this.effects.put("eff_withhealth_sec", this.getDefEffect2("With Health Sector","Health Sector affected?", 10, "yes-no"));
		this.effects.put("eff_witheducation_sec", this.getDefEffect2("With Education Sector","Sector affected?", 5, "yes-no"));
		this.effects.put("eff_withagricultural_sec", this.getDefEffect2("With Agricultural Sector","Sector affected?", 5, "yes-no"));
		this.effects.put("eff_withindustrial_sec", this.getDefEffect2("With Industrial Sector","Sector affected?", 5, "yes-no"));
		this.effects.put("eff_withwatersupply_sec", this.getDefEffect2("With water supply Sector","Sector affected?", 5, "yes-no"));
		this.effects.put("eff_withsewerage_sec", this.getDefEffect2("With Sewerage Sector","Sector affected?", 5, "yes-no"));
		this.effects.put("eff_withpowersupply_sec", this.getDefEffect2("With Power-energy Sector","Sector affected?", 5, "yes-no"));
		this.effects.put("eff_withcomm_sec", this.getDefEffect2("With Communication Sector","Sector affected?", 5, "yes-no"));
		this.effects.put("eff_transport_sec", this.getDefEffect2("With Transportation Sector","Sector affected?", 5, "yes-no"));
		do {
			this.effects.put("ext_" + rs.getString("id").toLowerCase(), 
				this.getDefEffect2(rs.getString("name"),rs.getString("description"), rs.getInt("length"), rs.getString("fieldtype")));
		} while (rs.next());
	}

	public String getCtycode() {
		return this.ctycode;
	}
    
	public HashMap<String, Object> getEffects() {
    	return this.effects;
	}

}
