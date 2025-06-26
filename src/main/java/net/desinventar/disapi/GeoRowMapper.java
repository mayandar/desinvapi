package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.RowMapper;

/* Datacards Row Mapper.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.2
 * */
public class GeoRowMapper implements RowMapper<Geo> {
	String country;
	
	GeoRowMapper(String cty) {
		this.country = cty;
	}
	String getCountry() {
		return this.country;
	}

	@Override
    public Geo mapRow(ResultSet rs, int line) throws SQLException {
		GeoResultSetExtractor extractor = new GeoResultSetExtractor(this.country);
        return extractor.extractData(rs);
	}

}
