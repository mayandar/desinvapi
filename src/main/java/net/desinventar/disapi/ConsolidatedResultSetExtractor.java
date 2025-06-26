package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.ResultSetExtractor;

/* Consolidated Resultset Extractor 
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.0
 * */
public class ConsolidatedResultSetExtractor implements ResultSetExtractor<Consolidated> {

	String country;
	String year;
	String indicator;
	
	ConsolidatedResultSetExtractor (String cty, String yr, String ind) {
		this.country = cty;
		this.year = yr;
		this.indicator = ind;
	}

    @Override
    public Consolidated extractData(ResultSet rs) throws SQLException {
    	Consolidated indicator = new Consolidated(this.country, this.year, this.indicator, rs);
        return indicator;
    }

}