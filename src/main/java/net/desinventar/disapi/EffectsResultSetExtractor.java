package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.ResultSetExtractor;

/* Datacards Resultset Extractor 
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.2
 * */
public class EffectsResultSetExtractor implements ResultSetExtractor<Effects> {

	String country;
	
	EffectsResultSetExtractor (String cty) {
		this.country = cty;
	}

    @Override
    public Effects extractData(ResultSet rs) throws SQLException {
    	Effects eff = new Effects(this.country, rs);
        return eff;
    }

}