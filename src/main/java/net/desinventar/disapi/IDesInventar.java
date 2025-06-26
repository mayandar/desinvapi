package net.desinventar.disapi;

import java.sql.SQLException;

public interface IDesInventar {
	void setRegion();
	Consolidated findIndicator(String country, String year, String indicator) throws SQLException;
	Datacards findDatacards(String country, String page) throws SQLException;
	//Datacards findDatacards(String country, String[] ids) throws SQLException;
	Effects findEffectsList(String country) throws SQLException;
	Geo findGeographyList(String country) throws SQLException;
	Hazards findHazardsList(String country) throws SQLException;
}
