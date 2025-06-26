package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.RowMapper;

/* Datacards Row Mapper.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.2
 * */
public class EffectsRowMapper implements RowMapper<Effects> {
	String country;
	
	EffectsRowMapper(String cty) {
		this.country = cty;
	}
	String getCountry() {
		return this.country;
	}

	@Override
    public Effects mapRow(ResultSet rs, int line) throws SQLException {
		EffectsResultSetExtractor extractor = new EffectsResultSetExtractor(this.country);
        return extractor.extractData(rs);
	}

}
