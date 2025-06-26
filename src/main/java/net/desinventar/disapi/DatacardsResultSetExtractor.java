package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.ResultSetExtractor;

/* Datacards Resultset Extractor 
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.2
 * */
public class DatacardsResultSetExtractor implements ResultSetExtractor<Datacards> {

	String country;
	String page;
	
	DatacardsResultSetExtractor (String cty, String pg) {
		this.country = cty;
		this.page = pg;
	}

    @Override
    public Datacards extractData(ResultSet rs) throws SQLException {
    	Datacards dcs = new Datacards(this.country, this.page, rs);
        return dcs;
    }

}