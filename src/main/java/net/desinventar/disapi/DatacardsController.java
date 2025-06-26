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
public class DatacardsController {
	@GetMapping("/datacards")
	public Datacards datacards(	@RequestParam(defaultValue = "x") String country, 
								@RequestParam(defaultValue = "0") String page) throws SQLException {
		DesInventar dao = new DesInventar(country);
		return dao.findDatacards(country, page);
	}
}
