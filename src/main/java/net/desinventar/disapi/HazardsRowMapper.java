package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.RowMapper;

/* Datacards Row Mapper.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.2
 * */
public class HazardsRowMapper implements RowMapper<Hazards> {
	String country;
	
	HazardsRowMapper(String cty) {
		this.country = cty;
	}
	String getCountry() {
		return this.country;
	}

	@Override
    public Hazards mapRow(ResultSet rs, int line) throws SQLException {
		HazardsResultSetExtractor extractor = new HazardsResultSetExtractor(this.country);
        return extractor.extractData(rs);
	}

}
