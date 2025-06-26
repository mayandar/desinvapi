package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.ResultSetExtractor;

/* Datacards Resultset Extractor 
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.2
 * */
public class GeoResultSetExtractor implements ResultSetExtractor<Geo> {

	String country;
	
	GeoResultSetExtractor (String cty) {
		this.country = cty;
	}

    @Override
    public Geo extractData(ResultSet rs) throws SQLException {
    	Geo g = new Geo(this.country, rs);
        return g;
    }

}