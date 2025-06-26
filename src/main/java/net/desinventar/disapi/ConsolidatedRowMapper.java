package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.RowMapper;

/* Consolidated Row Mapper.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.0
 * */
public class ConsolidatedRowMapper implements RowMapper<Consolidated> {
	
	String country;
	String year;
	String indicator;
	
	ConsolidatedRowMapper(String cty, String yr, String ind) {
		this.country = cty;
		this.year = yr;
		this.indicator = ind;
	}
	String getCountry() {
		return this.country;
	}
	String getYear() {
		return this.year;
	}
	String getIndicator() {
		return this.indicator;
	}

	@Override
    public Consolidated mapRow(ResultSet rs, int line) throws SQLException {
		ConsolidatedResultSetExtractor extractor = new ConsolidatedResultSetExtractor(this.country, this.year, this.indicator);
        return extractor.extractData(rs);
	}

}
