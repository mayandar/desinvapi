package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.ResultSetExtractor;

/* Datacards Resultset Extractor 
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.2
 * */
public class HazardsResultSetExtractor implements ResultSetExtractor<Hazards> {

	String country;
	
	HazardsResultSetExtractor (String cty) {
		this.country = cty;
	}

    @Override
    public Hazards extractData(ResultSet rs) throws SQLException {
    	Hazards hzd = new Hazards(this.country, rs);
        return hzd;
    }

}