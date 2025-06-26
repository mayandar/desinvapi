package net.desinventar.disapi;

import java.sql.SQLException;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/* Datacards controller - Rest service configuration.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.2
 * */
@RestController
public class GeoController {
	@GetMapping("/geographylist")
	public Geo geographylist(@RequestParam(defaultValue = "x") String country) throws SQLException {
		DesInventar dao = new DesInventar(country);
		//System.out.println("LOSS: "+ country +"-"+ year + ":"+ indicator);
		return dao.findGeographyList(country);
	}
}
