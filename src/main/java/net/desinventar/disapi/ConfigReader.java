package net.desinventar.disapi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/* DesInventar Configuration file
 * @author Mario A. Yandar <mayandar@gmail.com>
 * @version 1.0
 * */
/* Read the DesInventar properties file to get the DB URL, User and Password for the main DB
 * db.password=
 * db.username=
 * db.name=
 * */
public class ConfigReader {
    private static final Pattern LINE_PATTERN = Pattern.compile( "([^ =]+)[ =]?(.*)" );
    private static Map<String, String> properties = new HashMap<String, String>();
    /*{{  put( "db.name", "jdbc:postgresql://desinventar_db/desinventar_main" );
        put( "db.username", "postgres" );
        put( "db.password", "c0l0mbia98" );
    }};*/
    private static final String fileName = "/etc/desinventar/desinventar.properties";
    
    /* Main constructor: Read the configuration file and the lines in the hashmap
     * @param country - Country codification ISO3
     */
    public ConfigReader() {
        /*v*/ BufferedReader reader = null;
        try {
            reader = new BufferedReader( new FileReader( fileName ) );
            for ( String line; null != ( line = reader.readLine() );  ) {
                final Matcher matcher = LINE_PATTERN.matcher( line );
                if ( ! matcher.matches() ) {
                    System.err.println( "Bad config line: " + line );
                    return;
                }
                final String key   = matcher.group( 1 ).trim().toLowerCase();
                final String value = matcher.group( 2 ).trim();
                properties.put( key, value );
            }
        } catch ( final IOException x ) {
            throw new RuntimeException( "Error: " + x, x );
        } finally {
            if ( null != reader ) try {
                reader.close();
            } catch ( final IOException x2 ) {
                System.err.println( "Could not close " + fileName + " - " + x2 );
            }
        }
    }

    /* Get the name of the default administrator database
     * */
	public String getDB() {
		String value = "jdbc:postgresql://desinventar_db/desinventar_main";
		for (Map.Entry<String, String> entry: properties.entrySet()) {
			if (entry.getKey().equals("db.name"))
				value = entry.getValue();
		}
        //System.out.println(value);
		return value;
    }

    /* Get the name of the default user
     * */
	public String getUser() {
		String value = "postgres";
		for (Map.Entry<String, String> entry: properties.entrySet()) {
			if (entry.getKey().equals("db.username"))
				value = entry.getValue();
		}
        //System.out.println(value);
		return value;
    }

	/* Get the name of the default password
     * */
	public String getPassword() {
		String value = "c0l0mbia98";
		for (Map.Entry<String, String> entry: properties.entrySet()) {
			if (entry.getKey().equals("db.password"))
				value = entry.getValue();
		}
        //System.out.println(value);
		return value;
    }
}
