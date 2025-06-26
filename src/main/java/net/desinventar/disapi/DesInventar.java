package net.desinventar.disapi;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

//import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
//import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/* DesInventar Region Queries and main database. Prepare de SQL query to get the data of the indicators
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.0
 * */
public class DesInventar implements IDesInventar {
	private String region;
	private String country;
	private DataSource dataSource;
	private String url = "";
	private String driver = "";
	private String user = "";
	private String pass = "";
	private boolean isSendai;

    /* Main constructor: Main database connection to get the connection parameters for the database
     * @param country - Country codification ISO3
     */
    public DesInventar(String country) {
		final String server = "desinventar_db"; //desinventar_db
		this.country = country;
        // Special manage to Pacific Island Region (22 active territories)
        if (country.equals("asm") || country.equals("cok") || country.equals("etm") || country.equals("fsm") || country.equals("fji") || 
        	country.equals("pyf") || country.equals("gum") || country.equals("kir") || country.equals("mhl") || country.equals("nru") ||
        	country.equals("ncl") || country.equals("niu") || country.equals("mnp") || country.equals("plw") || country.equals("png") ||
        	country.equals("wsm") || country.equals("slb") || country.equals("tkl") || country.equals("ton") || country.equals("tuv") ||
        	country.equals("vut") || country.equals("wlf") ) {
	    	this.region = "pac"; // Pacific 
		    this.url = "jdbc:postgresql://"+ server +"/pacific";
		    this.driver = "org.postgresql.Driver";
		    this.user = "postgres";
		    this.pass = "c0l0mbia98"; // c0l0mbia98- PAC is a special database. 
        }
        // Other regions/countries:
		else {
			//ConfigReader C = new ConfigReader();
	    	this.region = country;
	    	DriverManagerDataSource ds = new DriverManagerDataSource();
	        ds.setDriverClassName("org.postgresql.Driver");
	        ds.setUrl("jdbc:postgresql://localhost/desinventar_main"); //desinventar_db
	        ds.setUsername("postgres");
	        ds.setPassword("postgres"); //c0l0mbia98
			JdbcTemplate main = new JdbcTemplate(ds);
			String sql = "select sdatabasename, sdriver, susername, spassword from country where scountryid='"+ country +"'";
			List<Map<String, Object>> rows = main.queryForList(sql);
			for (Map<String, Object> row : rows) {
			    this.url = row.get("sdatabasename").toString();
			    this.driver = row.get("sdriver").toString();
			    this.user = row.get("susername").toString();
			    this.pass = row.get("spassword").toString();
			}
		}
		setRegion();
    }
    
    /**
    * Set the Driver, URL and credentials in the DataSource Object to 
    * enable the connection to the specific DesInventar Region
    *
    * @return      void
    */
    @Override
	public void setRegion() {
		if (this.driver.isEmpty()) {
			this.dataSource = new DriverManagerDataSource();
			this.isSendai = false;
		}
		else {
	    	DriverManagerDataSource dataSource = new DriverManagerDataSource();
	        dataSource.setDriverClassName(this.driver);
	        dataSource.setUrl(this.url);
	        dataSource.setUsername(this.user);
	        dataSource.setPassword(this.pass);
	        // Inject the datasource into the dao
	        this.dataSource = dataSource;
	        this.isSendai = checkDIType();
		}
		//System.out.println("dao:: ("+ this.driver +") "+ this.url + ":"+ this.user + ":"+ this.pass);
    }
	
    /**
    * Verified the DesInventar type: classical or Sendai 
    *
    * @return    True, False 
    */
	private boolean checkDIType() {
		boolean type = false;
		JdbcTemplate dicc = new JdbcTemplate(this.dataSource);
		String sql = "SELECT count(1) as DIS FROM diccionario where nombre_campo like 'LIVELIHOOD%' or nombre_campo like 'DEATHS%' or nombre_campo like 'MISSING%' or nombre_campo like 'INJURED%'";
        List<Map<String, Object>> rows = dicc.queryForList(sql);
        for (Map<String, Object> row : rows) {
        	// Total result if is Sendai...
            if (((Long) row.get("DIS")).intValue() == 29) {
				type = true;
            }
        }
        return type;
	}
	
    /**
    * find the List of hazards for the Region 
    * @param	IsoCode(3) for the country
    * @return   Hazards Object
    */
	@SuppressWarnings("deprecation")
	@Override
	public Hazards findHazardsList(String country) throws SQLException {
		if (! this.driver.isEmpty()) {
			String sql = "SELECT v.evento as name, (SELECT nombre_en FROM eventos WHERE nombre=v.evento) as event, (SELECT descripcion FROM eventos WHERE nombre=v.evento)"
			+ " as desc, string_agg(causa, ',') as causes FROM (select evento, causa FROM fichas WHERE approved=0 AND evento is not null GROUP BY evento, causa) as v GROUP BY v.evento";
			JdbcTemplate select = new JdbcTemplate(this.dataSource);
			System.out.println("--"+ sql +"--");
			try {
				return select.queryForObject(sql, new Object[]{}, new HazardsRowMapper(country));
			}
			catch (Exception e) {
				System.out.println ("ERR: "+ e.getMessage());
				return null;
			}
		}
		return null;
	}

    /**
    * find the Geography: Levels and Units for the Region 
    * @param	IsoCode(3) for the country
    * @return   Geography Object
    */
	@SuppressWarnings("deprecation")
	@Override
	public Geo findGeographyList(String country) throws SQLException {
		if (! this.driver.isEmpty()) {
			String sql = "SELECT '0' as lev, lev0_cod as code, '' as parent, " +
				"lev0_name_en as label, lev0_name as name, filename, lev_code, lev_name FROM lev0, level_maps "+
				"WHERE map_level=0 UNION "+
				"SELECT '1' as lev, lev1_cod as code, lev1_lev0 as parent, "+
				"lev1_name_en as label, lev1_name as name, filename, lev_code, lev_name FROM lev1, lev0, level_maps "+
				"WHERE (lev1_lev0=lev0_cod or lev1_lev0 is null) AND map_level=1 UNION "+
				"SELECT '2' as lev, lev2_cod as code, lev2_lev1 as parent, "+
				"lev2_name_en as label, lev2_name as name, filename, lev_code, lev_name  FROM lev2, lev1, level_maps "+
				"WHERE (lev2_lev1=lev1_cod or lev2_lev1 is null) AND map_level=2 ORDER BY lev, code";
/*					
					"SELECT '0' as lev, n.descripcion_en as levlabel, n.descripcion as levname, lev0_cod as code, "
				+ "'' as parent, lev0_name_en as label, lev0_name as name, filename, lev_code, lev_name "
				+ "FROM niveles n, lev0, level_maps WHERE n.nivel=0 AND n.nivel=map_level "
				+ "UNION "
				+ "SELECT '1' as lev, n.descripcion_en as levlabel, n.descripcion as levname, lev1_cod as code, "
				+ "lev1_lev0 as parent, lev1_name_en as label, lev1_name as name, filename, lev_code, lev_name "
				+ "FROM niveles n, lev1, lev0, level_maps WHERE n.nivel=1 "
				+ "AND (lev1_lev0=lev0_cod or lev1_lev0 is null) AND n.nivel=map_level "
				+ "UNION "
				+ "SELECT '2' as lev, n.descripcion_en as levlabel, n.descripcion as levname, lev2_cod as code, "
				+ "lev2_lev1 as parent, lev2_name_en as label, lev2_name as name, filename, lev_code, lev_name  "
				+ "FROM niveles n, lev2, lev1, level_maps WHERE n.nivel=2 "
				+ "AND (lev2_lev1=lev1_cod or lev2_lev1 is null) AND n.nivel=map_level ORDER BY lev, code";*/
			JdbcTemplate select = new JdbcTemplate(this.dataSource);
			System.out.println("--"+ sql +"--");
			try {
				return select.queryForObject(sql, new Object[]{}, new GeoRowMapper(country));
			}
			catch (Exception e) {
				System.out.println ("ERR: "+ e.getMessage());
				return null;
			}
		}
		return null;
	}

    /**
    * find the List of Effects (Basic and extended) for the Region 
    * @param	IsoCode(3) for the country
    * @return   True, False 
    */
	@SuppressWarnings("deprecation")
	@Override
	public Effects findEffectsList(String country) throws SQLException {
		if (! this.driver.isEmpty()) {
			String sql = "SELECT nombre_campo as id, label_campo_en as label, label_campo as name, descripcion_campo as description, "
					+ "lon_x as length, CASE WHEN fieldtype=0 THEN 'varchar' WHEN fieldtype=1 THEN 'integer' "
					+ "WHEN fieldtype=2 THEN 'float' WHEN fieldtype=3 THEN 'currency' WHEN fieldtype=4 THEN 'date' "
					+ "WHEN fieldtype=5 THEN 'text' WHEN fieldtype=6 THEN 'yes-no' WHEN fieldtype=7 THEN 'list' ELSE '' "
					+ "END AS fieldtype FROM diccionario ORDER BY orden";
			JdbcTemplate select = new JdbcTemplate(this.dataSource);
			System.out.println("--"+ sql +"--");
			try {
				return select.queryForObject(sql, new Object[]{}, new EffectsRowMapper(country));
			}
			catch (Exception e) {
				System.out.println ("ERR: "+ e.getMessage());
				return null;
			}
		}
		return null;
	}

    /**
    * find the List of Datacards for the Region 
    * @param	IsoCode(3) for the country
    * @param	Page 
    * @return   Datacards Object
    */
	@SuppressWarnings("deprecation")
	@Override
	public Datacards findDatacards(String country, String page) throws SQLException {
		if (! (this.driver.isEmpty() || page.equals("nonext"))) {
			String sql = "SELECT (select count(1) from fichas where approved=0) as total_datacards, * FROM (select "
				+ "f.uu_id as uuid, f.level0 as geo_cod0, f.level1 as geo_cod1, f.level2 as geo_cod2, f.name0 as geo_nam0, "
				+ "f.name1 as geo_nam1, f.name2 as geo_nam2, f.lugar as geo_place, f.latitude as geo_lat, f.longitude as geo_lon, "
				+ "v.nombre_en as hzd_label, f.evento as hzd_event, f.duracion as hzd_eventduration, f.causa as hzd_cause, f.descausa as hzd_causedesc, "
				+ "f.magnitud2 as hzd_eventmagnitude, f.serial as rec_serial, f.fuentes as rec_sources, f.fechapor as rec_user, "
				+ "f.fechafec as rec_date, f.fechano as eff_year, f.fechames as eff_month, f.fechadia as eff_day, "
				+ "f.muertos as eff_deaths, f.hay_muertos as eff_withdeaths, "
				+ "f.heridos as eff_injured, f.hay_heridos as eff_withinjured, "
				+ "f.desaparece as eff_missing, f.hay_deasparece as eff_withmissing, "
				+ "f.afectados as eff_indirectaffected, f.hay_afectados as eff_withindirecaffected, "
				+ "f.damnificados as eff_directaffected, f.hay_damnificados as eff_withdirectaffected, "
				+ "f.vivdest as eff_dstdwelling, f.hay_vivdest as eff_withdstdwelling, "
				+ "f.vivafec as eff_dmgdwelling, f.hay_vivafec as eff_withdmgdwelling, "
				+ "f.valorloc as eff_losseslcu, f.valorus as eff_lossesusd, "
				+ "f.otros as eff_others, f.hay_otros as eff_withothers, "
				+ "f.evacuados as eff_evacuated, f.hay_evacuados as eff_withevacuated, "
				+ "f.reubicados as eff_relocated, f.hay_reubicados as eff_withrelocated, "
				+ "f.socorro as eff_reliefcenters, f.socorro as eff_withrelief_sec, "
				+ "f.nhospitales as eff_healthcenters, f.salud as eff_withhealth_sec, "
				+ "f.nescuelas as eff_educationcenters, f.educacion as eff_witheducation_sec, "
				+ "f.nhectareas as eff_crops, f.cabezas as eff_livestock, f.agropecuario as eff_withagricultural_sec, "
				+ "f.industrias as eff_withindustrial_sec, f.acueducto as eff_withwatersupply_sec, "
				+ "f.alcantarillado as eff_withsewerage_sec, f.energia as eff_withpowersupply_sec, "
				+ "f.comunicaciones as eff_withcomm_sec, f.di_comments as rec_comments, "
				+ "f.kmvias as eff_roadskm, f.transporte as eff_transport_sec, e.* FROM fichas f, extension e, eventos v "
				+ "WHERE f.clave=e.clave_ext AND v.nombre = f.evento AND f.approved=0 ORDER BY f.fechano, f.fechames, f.fechadia, f.serial "
				+ "LIMIT 1000 OFFSET "+ page +"*1000)";
			JdbcTemplate select = new JdbcTemplate(this.dataSource);
			System.out.println("--"+ sql +"--");
			try {
				return select.queryForObject(sql, new Object[]{}, new DatacardsRowMapper(country, page));
			}
			catch (Exception e) {
				System.out.println ("ERR: "+ e.getMessage());
				return null;
			}
		}
		return null;
	}

	/**
    * find the List of Datacards for the Region 
    * @param	IsoCode(3) for the country
    * @param	Begin date of the datacards
    * @param	End date of the datacards
    * @return   Datacards Object
    */
/*	@SuppressWarnings("deprecation")
	@Override
	public Datacards findDatacards(String country, String dateini, String dateend) throws SQLException {
		if (! this.driver.isEmpty()) {
			String sql = "SELECT f.uu_id as uuid, f.level0 as geo_cod0, f.level1 as geo_cod1, f.level2 as geo_cod2, f.name0 as geo_nam0, "
					+ "f.name1 as geo_nam1, f.name2 as geo_nam2, f.lugar as geo_place, f.latitude as geo_lat, f.longitude as geo_lon, "
					+ "v.nombre_en as hzd_label, f.evento as hzd_event, f.duracion as hzd_eventduration, f.causa as hzd_cause, f.descausa as hzd_causedesc, "
					+ "f.magnitud2 as hzd_eventmagnitude, f.serial as rec_serial, f.fuentes as rec_sources, f.fechapor as rec_user, "
					+ "f.fechafec as rec_date, f.fechano as eff_year, f.fechames as eff_month, f.fechadia as eff_day, "
					+ "f.muertos as eff_deaths, f.hay_muertos as eff_withdeaths, "
					+ "f.heridos as eff_injured, f.hay_heridos as eff_withinjured, "
					+ "f.desaparece as eff_missing, f.hay_deasparece as eff_withmissing, "
					+ "f.afectados as eff_indirectaffected, f.hay_afectados as eff_withindirecaffected, "
					+ "f.damnificados as eff_directaffected, f.hay_damnificados as eff_withdirectaffected, "
					+ "f.vivdest as eff_dstdwelling, f.hay_vivdest as eff_withdstdwelling, "
					+ "f.vivafec as eff_dmgdwelling, f.hay_vivafec as eff_withdmgdwelling, "
					+ "f.valorloc as eff_losseslcu, f.valorus as eff_lossesusd, "
					+ "f.otros as eff_others, f.hay_otros as eff_withothers, "
					+ "f.evacuados as eff_evacuated, f.hay_evacuados as eff_withevacuated, "
					+ "f.reubicados as eff_relocated, f.hay_reubicados as eff_withrelocated, "
					+ "f.socorro as eff_reliefcenters, f.socorro as eff_withrelief_sec, "
					+ "f.nhospitales as eff_healthcenters, f.salud as eff_withhealth_sec, "
					+ "f.nescuelas as eff_educationcenters, f.educacion as eff_witheducation_sec, "
					+ "f.nhectareas as eff_hectares, f.cabezas as eff_livestock, f.agropecuario as eff_withagricultural_sec, "
					+ "f.industrias as eff_withindustrial_sec, f.acueducto as eff_withwatersupply_sec, "
					+ "f.alcantarillado as eff_withsewerage_sec, f.energia as eff_withpowersupply_sec, "
					+ "f.comunicaciones as eff_withcomm_sec, f.di_comments as rec_comments, "
					+ "f.kmvias as eff_roadskm, f.transporte as eff_transport_sec, e.* FROM fichas f, extension e, eventos v "
					+ "WHERE f.clave=e.clave_ext AND v.nombre = f.evento AND approved=0";
			JdbcTemplate select = new JdbcTemplate(this.dataSource);
			// DATES processing: format YYYY-MM-DD
			String[] dt1 = dateini.split("-");
			String[] dt2 = dateend.split("-");
			// checking date range
			if (dt1.length == 3 && dt2.length == 3)
				sql += " AND (f.fechano*10000)+(f.fechames*100)+f.fechadia BETWEEN "+ dt1[0]+dt1[1]+dt1[2] +" AND "+ dt2[0]+dt2[1]+dt2[2];
			else if (dt1.length != 3 && dt2.length == 3)
				sql += " AND (f.fechano*10000)+(f.fechames*100)+f.fechadia <= "+ dt2[0]+dt2[1]+dt2[2];
			else if (dt1.length == 3 && dt2.length != 3)
				sql += " AND (f.fechano*10000)+(f.fechames*100)+f.fechadia >= "+ dt1[0]+dt1[1]+dt1[2];
			else
				System.out.println("ERR: Date format incorrect - Searching all database");
			// sql += " ORDER BY f.fechano, f.fechames, f.fechadia";
			System.out.println("--"+ sql +"--");
			try {
				return select.queryForObject(sql, new Object[]{}, new DatacardsRowMapper(country, dateini, dateend));
			}
			catch (Exception e) {
				System.out.println ("ERR: "+ e.getMessage());
				return null;
			}
		}
		return null;
	}
*/	
    /**
    * find the List of Datacards for the Region 
    * @param	IsoCode(3) for the country
    * @param	Begin date of the datacards
    * @param	End date of the datacards
    * @return   Datacards Object
    */
/*	@SuppressWarnings("deprecation")
	@Override
	public Datacards findDatacards(String country, String[] ids) throws SQLException {
		if (! this.driver.isEmpty()) {
			String sql = "SELECT f.*, e.* FROM fichas f, extension e where f.clave=e.clave_ext";
			JdbcTemplate select = new JdbcTemplate(this.dataSource);
			if (ids.length > 0) {
				sql += " AND uu_id IN ("+ dt1[0]+dt1[1]+dt1[2] +")";
			}
			else {
				System.out.println("ERR: No Ids requested:(");
			}
			System.out.println("--"+ sql +"--");
			try {
				return select.queryForObject(sql, new Object[]{}, new DatacardsRowMapper(country, dateini, dateend));
			}
			catch (Exception e) {
				System.out.println ("ERR: "+ e.getMessage());
				return null;
			}
		}
		return null;
	}*/

	/**
    * find the Indicator (Sendai) for the Region 
    * @param	IsoCode(3) for the country
    * @param	Begin date of the datacards
    * @param	End date of the datacards
    * @return   Datacards Object
    */
	@SuppressWarnings("deprecation")
	@Override
	public Consolidated findIndicator(String country, String year, String indicator) throws SQLException {
		if (! this.driver.isEmpty()) {
			String sql = "";
			JdbcTemplate select = new JdbcTemplate(this.dataSource);
			switch (indicator) {
				case "a2a":	sql = sqla2a(year);		break; // total + haz + geo + other(s,a,i,d)
				case "a3a":	sql = sqla3a(year);		break; // total + haz + geo + other(s,a,i,d)
				case "b2":	sql = sqlb2(year);		break; // total + haz + geo + other(s,a,i,d)
				case "b3":	sql = sqlb3(year);		break; // total + haz + geo + other(s,a,i,d)
				case "b3a":	sql = sqlb3a(year);		break; // total + haz + geo
				case "b4":	sql = sqlb4(year);		break; // total + haz + geo + other(s,a,i,d)
				case "b4a":	sql = sqlb4a(year);		break; // total + haz + geo
				case "b5":	sql = sqlb5(year);		break; // total + haz + geo + other(s,a,i,d)
				case "c2c":	sql = sqlc2c(year);		break; // ($, total, dmg, dty) + haz + geo + other(crops)
				case "c2l":	sql = sqlc2l(year);		break; // ($, total, dmg, dty) + haz + geo + other(livestock)
				case "c2fo":sql = sqlc2fo(year);	break; // ($, total, dmg, dty) + haz + geo -> forestry
				case "c2a":	sql = sqlc2a(year);		break; // ($, total, dmg, dty) + haz + geo -> aquaculture
				case "c2fi":sql = sqlc2fi(year);	break; // ($, total, dmg, dty) + haz + geo -> fisheries
				case "c2lb":sql = sqlc2lb(year);	break; // ($, total, dmg, dty) + haz + geo -> stocks
				case "c2la":sql = sqlc2la(year);	break; // ($, total, dmg, dty) + haz + geo -> assets
				case "c3":	sql = sqlc3(year);		break; // ($, total, dmg, dty) + haz + geo + other(productive)
				case "c4":	sql = sqlc4(year);		break; // ($, total, dmg, dty) + haz + geo -> housing
				case "c5a":	sql = sqlc5a(year);		break; // ($, total, dmg, dty) + haz + geo -> health-d2
				case "c5b":	sql = sqlc5b(year);		break; // ($, total, dmg, dty) + haz + geo -> education-d3
				case "c5c":	sql = sqlc5c(year);		break; // ($, total, dmg, dty) + haz + geo + other(infrastructure)
				case "c6":	sql = sqlc6(year);		break; // ?number (a-g)* 7 + haz + geo -> cultural heritage
				case "d6":	sql = sqld6(year);		break; // ?number (d6,d7,d8*) 13 + haz + geo -> services disrupted
				case "d7":	sql = sqld7(year);		break; // ?number + haz + geo -> health services disrupted
				case "d8":	sql = sqld8(year);		break; // ?number + haz + geo -> education services disrupted
			}
			System.out.println("--"+ sql +"--");
			if (sql.isEmpty()) {
				System.out.println ("ERR: Indicator not available");
				return null;
			}
			else {
				try {
					return select.queryForObject(sql, new Object[]{}, new ConsolidatedRowMapper(country, year, indicator));
				}
				catch (Exception e) {
					System.out.println ("ERR: "+ e.getMessage());
					return null;
				}
			}
		}
		else {
			System.out.println ("ERR: Country not available");
			return null;
		}
	}
	
	/* Deaths */
	private String sqla2a(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = "and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'a2a' as category, sum(muertos) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'women' as category, sum(deaths_female) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'men' as category, sum(deaths_male) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Children (0-14)' as category, sum(deaths_children) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Adults (15-64)' as category, sum(deaths_adults) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Seniors (65 +)' as category, sum(deaths_elder) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'disability' as type, 'Persons with disability' as category, sum(deaths_disabled) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'income' as type, 'Under national poverty line' as category, sum(deaths_poor) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(muertos) as number from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(muertos) as number from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'a2a' as category, sum(muertos) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(muertos) as number from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(muertos) as number from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Missings */
	private String sqla3a(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = "and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'a3a' as category, sum(desaparece) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'women' as category, sum(missing_female) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'men' as category, sum(missing_male) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Children (0-14)' as category, sum(missing_children) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Adults (15-64)' as category, sum(missing_adults) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Seniors (65 +)' as category, sum(missing_elder) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'disability' as type, 'Persons with disability' as category, sum(missing_disabled) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'income' as type, 'Under national poverty line' as category, sum(missing_poor) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(desaparece) as number from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(desaparece) as number from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'a3a' as category, sum(desaparece) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(desaparece) as number from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(desaparece) as number from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Ill-Injured */
	private String sqlb2(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = "and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'b2' as category, sum(heridos) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'women' as category, sum(injured_female) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'men' as category, sum(injured_male) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Children (0-14)' as category, sum(injured_children) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Adults (15-64)' as category, sum(injured_adults) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Seniors (65 +)' as category, sum(injured_elder) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'disability' as type, 'Persons with disability' as category, sum(injured_disabled) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'income' as type, 'Under national poverty line' as category, sum(injured_poor) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(heridos) as number from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(heridos) as number from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'b2' as category, sum(heridos) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(heridos) as number from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(heridos) as number from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* living dwellings damaged */
	private String sqlb3(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = "and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'b3' as category, sum(living_dmgd_dwellings) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'women' as category, sum(living_dmgd_dwellings_female) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'men' as category, sum(living_dmgd_dwellings_male) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Children (0-14)' as category, sum(living_dmgd_dwellings_children) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Adults (15-64)' as category, sum(living_dmgd_dwellings_adults) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Seniors (65 +)' as category, sum(living_dmgd_dwellings_elder) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'disability' as type, 'Persons with disability' as category, sum(living_dmgd_dwellings_disabled) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'income' as type, 'Under national poverty line' as category, sum(living_dmgd_dwellings_poor) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(living_dmgd_dwellings) as number from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(living_dmgd_dwellings) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'b3' as category, '0' as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, '0' as number from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, '0' as number from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* dwellings damaged */
	private String sqlb3a(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = "and level0='"+ this.country.toUpperCase() +"'";
		return "select 'general' as type, 'b3a' as category, sum(vivafec) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(vivafec) as number from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(vivafec) as number from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* living dwellings destroyed */
	private String sqlb4(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = "and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'b4' as category, sum(living_dstr_dwellings) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'women' as category, sum(living_dstr_dwellings_female) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'men' as category, sum(living_dstr_dwellings_male) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Children (0-14)' as category, sum(living_dstr_dwellings_children) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Adults (15-64)' as category, sum(living_dstr_dwellings_adults) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Seniors (65 +)' as category, sum(living_dstr_dwellings_elder) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'disability' as type, 'Persons with disability' as category, sum(living_dstr_dwellings_disabled) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'income' as type, 'Under national poverty line' as category, sum(living_dstr_dwellings_poor) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(living_dstr_dwellings) as number from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(living_dstr_dwellings) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'b4' as category, '0' as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, '0' as number from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, '0' as number from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* dwellings destroyed */
	private String sqlb4a(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = "and level0='"+ this.country.toUpperCase() +"'";
		return "select 'general' as type, 'b4a' as category, sum(vivdest) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(vivdest) as number from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(vivdest) as number from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* impact in Livelihoods */
	private String sqlb5(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = "and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'b5' as category, sum(livelihood_afctd) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'women' as category, sum(livelihoods_female) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'sex' as type, 'men' as category, sum(livelihoods_male) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Children (0-14)' as category, sum(livelihoods_children) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Adults (15-64)' as category, sum(livelihoods_adults) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'age' as type, 'Seniors (65 +)' as category, sum(livelihoods_elder) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'disability' as type, 'Persons with disability' as category, sum(livelihoods_disabled) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'income' as type, 'Under national poverty line' as category, sum(livelihoods_poor) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(livelihood_afctd) as number from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(livelihood_afctd) as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'b5' as category, '0' as number from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, '0' as number from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, '0' as number from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Agricultural Crop Loss */
	private String sqlc2c(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = "and level0='"+ this.country.toUpperCase() +"'";
		String sql;
		if (this.isSendai) {
			sql = "select 'general' as type, 'c2c' as category, sum(nhectareas) as total, "+
				"sum(ha_dmgd) as damaged, sum(ha_dstr) as destroyed, sum(loss_crops) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(nhectareas) as total, " + 
				"sum(ha_dmgd) as damaged, sum(ha_dstr) as destroyed, sum(loss_crops) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION ";
			List<String> crops = getListofOther("crops");
			for (String i : crops) {
				sql = sql + "select 'crops' as type, '"+ i +"' as category, sum(TOTAL_"+ i +") as total, sum(DAMAGED_"+ i +") as damaged, "+
					"sum(DESTRYD_"+ i +") as destroyed, sum(LOSS_"+ i +") as economic from fichas, extension where clave=clave_ext "+
					"and approved=0 and fechano="+ year + subreg +" UNION ";
			}
			sql = sql + "select 'geography' as type, name0 as category, sum(nhectareas) as total, " + 
			"sum(ha_dmgd) as damaged, sum(ha_dstr) as destroyed, sum(loss_crops) as economic from fichas, extension "+
			"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		}
		else {
			sql = "select 'general' as type, 'c2c' as category, sum(nhectareas) as total, '0' as damaged, '0' as destroyed, '0' as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(nhectareas) as total, '0' as damaged, '0' as destroyed, '0' as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(nhectareas) as total, '0' as damaged, '0' as destroyed, '0' as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		}
		return sql;
	}
	
	/* Agricultural Livestocks */
	private String sqlc2l(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = "and level0='"+ this.country.toUpperCase() +"'";
		String sql;
		if (this.isSendai) {
			sql = "select 'general' as type, 'c2l' as category, sum(livestock_total) as total, "+
				"sum(livestock_dmgd) as damaged, sum(cabezas) as destroyed, sum(loss_livestock_total) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(livestock_total) as total, sum(livestock_dmgd) as damaged, "+
				"sum(productive_assets_dstr) as destroyed, sum(loss_livestock_total) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION ";
			List<String> lstock = getListofOther("productive");
			for (String i : lstock) {
				sql = sql + "select 'livestocks' as type, '"+ i +"' as category, sum(TOTAL_"+ i +") as total, sum(DAMAGED_"+ i +") as damaged, "+
					"sum(DESTRYD_"+ i +") as destroyed, sum(LOSS_"+ i +") as economic from fichas, extension where clave=clave_ext "+
					"and approved=0 and fechano="+ year + subreg +" UNION ";
			}
			sql = sql + "select 'geography' as type, name0 as category, sum(livestock_total) as total, " + 
			"sum(livestock_dmgd) as damaged, sum(cabezas) as destroyed, sum(loss_livestock_total) as economic from fichas, extension "+
			"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		}
		else {
			sql = "select 'general' as type, 'c2l' as category, 0 as total, 0 as damaged, sum(cabezas) as destroyed, 0 as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, 0 as total, 0 as damaged, sum(cabezas) as destroyed, 0 as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, 0 as total, 0 as damaged, sum(cabezas) as destroyed, 0 as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		}
		return sql;
	}

	private List<String> getListofOther(String type) {
		List<String> list = new ArrayList<String>();
		JdbcTemplate dicc = new JdbcTemplate(this.dataSource);
		String sql = "SELECT substring(nombre_campo, 6) as code FROM diccionario where nombre_campo like 'LOSS_%'";
        List<Map<String, Object>> rows = dicc.queryForList(sql);
        for (Map<String, Object> row : rows) {
            try {
            	String code = row.get("code").toString();
                int number = Integer.parseInt(code);
                if (type.equals("crops") && number >= 1 && number <= 173)
                	list.add(code);
                else if (type.equals("livestocks") && number >= 174 && number <= 196)
                	list.add(code);
                else if (type.equals("productive") && number >= 216 && number <= 350)
                	list.add(code);
                else if (type.equals("infraestructure") && number >= 352 && number <= 484)
                	list.add(code);
            }
            catch (NumberFormatException ex){ }
        }
		return list;
	}

	/* Agricultural Forestry */
	private String sqlc2fo(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'c2fo' as category, sum(ha_forest_total) as total, "+
				"sum(ha_forest_dmgd) as damaged, sum(ha_forest_dstr) as destroyed, sum(loss_forest_total) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(ha_forest_total) as total, " + 
				"sum(ha_forest_dmgd) as damaged, sum(ha_forest_dstr) as destroyed, sum(loss_forest_total) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
				"select 'geography' as type, name0 as category, sum(ha_forest_total) as total, " + 
				"sum(ha_forest_dmgd) as damaged, sum(ha_forest_dstr) as destroyed, sum(loss_forest_total) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'c2fo' as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Agricultural Aquaculture */
	private String sqlc2a(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'c2a' as category, sum(ha_aquaculture_total) as total, "+
				"sum(ha_aquaculture_dmgd) as damaged, sum(ha_aquaculture_dstr) as destroyed, sum(loss_aquaculture_total) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(ha_aquaculture_total) as total, " + 
				"sum(ha_aquaculture_dmgd) as damaged, sum(ha_aquaculture_dstr) as destroyed, sum(loss_aquaculture_total) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
				"select 'geography' as type, name0 as category, sum(ha_aquaculture_total) as total, " + 
				"sum(ha_aquaculture_dmgd) as damaged, sum(ha_aquaculture_dstr) as destroyed, sum(loss_aquaculture_total) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'c2a' as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Agricultural Fisheries */
	private String sqlc2fi(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'c2fi' as category, sum(vessels_total) as total, "+
				"sum(vessels_dmgd) as damaged, sum(vessels_dstr) as destroyed, sum(loss_vessels_total) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(vessels_total) as total, " + 
				"sum(vessels_dmgd) as damaged, sum(vessels_dstr) as destroyed, sum(loss_vessels_total) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
				"select 'geography' as type, name0 as category, sum(vessels_total) as total, " + 
				"sum(vessels_dmgd) as damaged, sum(vessels_dstr) as destroyed, sum(loss_vessels_total) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'c2fi' as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Agricultural Stock */
	private String sqlc2lb(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'c2lb' as category, sum(stock_facilities_afctd) as total, "+
				"sum(stock_facilities_dmgd) as damaged, sum(stock_facilities_dstr) as destroyed, sum(stock_loss_afctd) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(stock_facilities_afctd) as total, " + 
				"sum(stock_facilities_dmgd) as damaged, sum(stock_facilities_dstr) as destroyed, sum(stock_loss_afctd) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
				"select 'geography' as type, name0 as category, sum(stock_facilities_afctd) as total, " + 
				"sum(stock_facilities_dmgd) as damaged, sum(stock_facilities_dstr) as destroyed, sum(stock_loss_afctd) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'c2lb' as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Agricultural Productive Assets */
	private String sqlc2la(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'c2la' as category, sum(agri_assets_afctd) as total, "+
				"sum(agri_assets_dmgd) as damaged, sum(agri_assets_dstr) as destroyed, sum(agri_assets_loss_afctd) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(agri_assets_afctd) as total, " + 
				"sum(agri_assets_dmgd) as damaged, sum(agri_assets_dstr) as destroyed, sum(agri_assets_loss_afctd) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
				"select 'geography' as type, name0 as category, sum(agri_assets_afctd) as total, " + 
				"sum(agri_assets_dmgd) as damaged, sum(agri_assets_dstr) as destroyed, sum(agri_assets_loss_afctd) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'c2la' as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Productive Assets */
	private String sqlc3(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		String sql;
		if (this.isSendai) {
			sql = "select 'general' as type, 'c3' as category, sum(productive_assets_afctd) as total, sum(productive_assets_dmgd) as damaged, "+
				"sum(productive_assets_dstr) as destroyed, sum(productive_assets_loss_afctd) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(productive_assets_afctd) as total, sum(productive_assets_dmgd) as damaged, "+
				"sum(productive_assets_dstr) as destroyed, sum(productive_assets_loss_afctd) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION ";
			List<String> lstock = getListofOther("productive");
			for (String i : lstock) {
				sql = sql + "select 'productive' as type, '"+ i +"' as category, sum(TOTAL_"+ i +") as total, sum(DAMAGED_"+ i +") as damaged, "+
					"sum(DESTRYD_"+ i +") as destroyed, sum(LOSS_"+ i +") as economic from fichas, extension where clave=clave_ext "+
					"and approved=0 and fechano="+ year + subreg +" UNION ";
			}
			sql = sql + "select 'geography' as type, name0 as category, sum(productive_assets_afctd) as total, sum(productive_assets_dmgd) as damaged, "+
			"sum(productive_assets_dstr) as destroyed, sum(productive_assets_loss_afctd) as economic from fichas, extension "+
			"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		}
		else {
			sql = "select 'general' as type, 'c3' as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		}
		return sql;
	}

	/* Housing Sector */
	private String sqlc4(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'c4' as category, sum(houses_total) as total, "+
				"sum(loss_dwellings_dmgd) as damaged, sum(loss__dwellingsdstr) as destroyed, sum(loss_dwellings) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(houses_total) as total, " + 
				"sum(loss_dwellings_dmgd) as damaged, sum(loss__dwellingsdstr) as destroyed, sum(loss_dwellings) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
				"select 'geography' as type, name0 as category, sum(houses_total) as total, " + 
				"sum(loss_dwellings_dmgd) as damaged, sum(loss__dwellingsdstr) as destroyed, sum(loss_dwellings) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'c4' as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Health Sector */
	private String sqlc5a(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'c5a' as category, sum(nhospitales) as total, sum(health_facilities_dmgd) as damaged, "+
				"sum(health_facilities_dstr) as destroyed, sum(loss_health_facilities) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(nhospitales) as total, sum(health_facilities_dmgd) as damaged, "+
				"sum(health_facilities_dstr) as destroyed, sum(loss_health_facilities) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
				"select 'geography' as type, name0 as category, sum(nhospitales) as total, sum(health_facilities_dmgd) as damaged, "+
				"sum(health_facilities_dstr) as destroyed, sum(loss_health_facilities) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'c5a' as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Education Sector */
	private String sqlc5b(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'c5b' as category, sum(nescuelas) as total, sum(education_dmgd) as damaged, "+
				"sum(education_dstr) as destroyed, sum(loss_education) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(nescuelas) as total, sum(education_dmgd) as damaged, "+
				"sum(education_dstr) as destroyed, sum(loss_education) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
				"select 'geography' as type, name0 as category, sum(nescuelas) as total, sum(education_dmgd) as damaged, "+
				"sum(education_dstr) as destroyed, sum(loss_education) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'c5b' as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* other infrastructure */
	private String sqlc5c(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		String sql;
		if (this.isSendai) {
			sql = "select 'general' as type, 'c5c' as category, sum(number_infrastructures) as total, sum(number_dmgd_infrastructures) as damaged, "+
				"sum(number_dstr_infrastructures) as destroyed, sum(loss_infrastructures) as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(number_infrastructures) as total, sum(number_dmgd_infrastructures) as damaged, "+
				"sum(number_dstr_infrastructures) as destroyed, sum(loss_infrastructures) as economic from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION ";
			List<String> lstock = getListofOther("infraestructure");
			for (String i : lstock) {
				sql = sql + "select 'infraestructure' as type, '"+ i +"' as category, sum(TOTAL_"+ i +") as total, sum(DAMAGED_"+ i +") as damaged, "+
					"sum(DESTRYD_"+ i +") as destroyed, sum(LOSS_"+ i +") as economic from fichas, extension where clave=clave_ext "+
					"and approved=0 and fechano="+ year + subreg +" UNION ";
			}
			sql = sql + "select 'geography' as type, name0 as category, sum(number_infrastructures) as total, sum(number_dmgd_infrastructures) as damaged, "+
			"sum(number_dstr_infrastructures) as destroyed, sum(loss_infrastructures) as economic from fichas, extension "+
			"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		}
		else {
			sql = "select 'general' as type, 'c5c' as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, 0 as total, 0 as damaged, 0 as destroyed, 0 as economic from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		}
		return sql;
	}
	
	/* Cultural Heritage */
	private String sqlc6(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'c6' as category, sum(loss_cultural_fixed) as c6a, sum(loss_cultural_mobile_dmgd) as c6b, "+
				"sum(loss_cultural_mobile_dstr) as c6c, sum(cultural_fixed_dmgd) as c6d, sum(cultural_fixed_dstr) as c6e, "+
				"sum(cultural_mobile_dmgd) as c6f, sum(cultural_mobile_dstr) as c6g from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(loss_cultural_fixed) as c6a, sum(loss_cultural_mobile_dmgd) as c6b, " + 
				"sum(loss_cultural_mobile_dstr) as c6c, sum(cultural_fixed_dmgd) as c6d, sum(cultural_fixed_dstr) as c6e, " + 
				"sum(cultural_mobile_dmgd) as c6f, sum(cultural_mobile_dstr) as c6g from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
				"select 'geography' as type, name0 as category, sum(loss_cultural_fixed) as c6a, sum(loss_cultural_mobile_dmgd) as c6b, " + 
				"sum(loss_cultural_mobile_dstr) as c6c, sum(cultural_fixed_dmgd) as c6d, sum(cultural_fixed_dstr) as c6e, " + 
				"sum(cultural_mobile_dmgd) as c6f, sum(cultural_mobile_dstr) as c6g from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'c6' as category, 0 as c6a, 0 as c6b, 0 as c6c, 0 as c6d, 0 as c6e, "+
				"0 as c6f, 0 as c6g from fichas, extension where clave=clave_ext and approved=0 and fechano="+ year;
	}

	/* Disruptions to Basic Services Education */
	private String sqld6(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		return "select 'general' as type, 'd6' as category, sum(educacion) as total from fichas, extension "+
			"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
			"select 'hazard' as type, evento as category, sum(educacion) as total from fichas, extension " +
			"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
			"select 'geography' as type, name0 as category, sum(educacion) as total from fichas, extension "+
			"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Disruptions to Basic Services Health */
	private String sqld7(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		return "select 'general' as type, 'd7' as category, sum(salud) as total from fichas, extension "+
			"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
			"select 'hazard' as type, evento as category, sum(salud) as total from fichas, extension " +
			"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
			"select 'geography' as type, name0 as category, sum(salud) as total from fichas, extension "+
			"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

	/* Disruptions to Basic Services (Electricity / power, Information and Communication Technology (ITC) system,
	Public administration services, Relief and Emergency Services, Sewage service, Solid waste service, 
	Transportation Services, Water supply) */
	private String sqld8(String year) {
		String subreg = "";
		if (this.region.equals("pac"))
			subreg = " and level0='"+ this.country.toUpperCase() +"'";
		if (this.isSendai)
			return "select 'general' as type, 'd8' as category, sum(transporte) as total from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " +
				"select 'hazard' as type, evento as category, sum(transporte) as total from fichas, extension " +
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by evento UNION "+
				"select 'geography' as type, name0 as category, sum(transporte) as total from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" group by name0 order by type";
		else
			return "select 'general' as type, 'd8' as category, sum(transporte)+sum(energia)+sum(comunicaciones)" +
				"+sum(acueducto)+sum(socorro)+sum(alcantarillado)+sum(PUBLIC_ADMIN_SERVICE)+sum(SOLID_WASTE_SERVICE) " +
				"as total from fichas, extension "+
				"where clave=clave_ext and approved=0 and fechano="+ year + subreg +" UNION " + 
				"select 'hazard' as type, evento as category, sum(transporte) as total from fichas " +
				"where approved=0 and fechano="+ year + subreg +" group by evento UNION " + 
				"select 'geography' as type, name0 as category, sum(transporte) as total from fichas "+
				"where approved=0 and fechano="+ year + subreg +" group by name0 order by type";
	}

}
