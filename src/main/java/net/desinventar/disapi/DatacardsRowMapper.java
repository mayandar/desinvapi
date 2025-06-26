package net.desinventar.disapi;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.RowMapper;

/* Datacards Row Mapper.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.2
 * */
public class DatacardsRowMapper implements RowMapper<Datacards> {
	String country;
	String page;
	
	DatacardsRowMapper(String cty, String pg) {
		this.country = cty;
		this.page = pg;
	}
	String getCountry() {
		return this.country;
	}
	String getPage() {
		return this.page;
	}

	@Override
    public Datacards mapRow(ResultSet rs, int line) throws SQLException {
		DatacardsResultSetExtractor extractor = new DatacardsResultSetExtractor(this.country, this.page);
        return extractor.extractData(rs);
	}

}
