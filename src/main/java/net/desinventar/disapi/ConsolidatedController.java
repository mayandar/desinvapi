package net.desinventar.disapi;

import java.sql.SQLException;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/* Consolidated controller - Rest service configuration.
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.0
 * */
@RestController
public class ConsolidatedController {

	@GetMapping("/consolidated")
	public Consolidated consolidated(@RequestParam(defaultValue = "x") String country, 
									 @RequestParam(defaultValue = "") String year,
									 @RequestParam(defaultValue = "") String indicator) throws SQLException{
		DesInventar dao = new DesInventar(country);
		System.out.println("LOSS: "+ country +"-"+ year + ":"+ indicator);
		return dao.findIndicator(country, year, indicator);
	}
}
