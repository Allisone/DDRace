/* CSqlScore class by Sushi */
#if defined(CONF_SQL)
#include <string.h>

#include <game/version.h>
#include <engine/shared/config.h>
#include "../entities/character.h"
#include "../gamemodes/DDRace.h"
#include <engine/server/server.h>
#include "sql_score.h"
#include <engine/shared/console.h>

static LOCK gs_SqlLock = 0;

CSqlScore::CSqlScore(CGameContext *pGameServer) : m_pGameServer(pGameServer),
m_pServer(pGameServer->Server()),
m_pDatabase(g_Config.m_SvSqlDatabase),
m_pPrefix(g_Config.m_SvSqlPrefix),
m_pDDRaceTablesPrefix(g_Config.m_SvSqlDDRacePrefix),
m_pUser(g_Config.m_SvSqlUser),
m_pPass(g_Config.m_SvSqlPw),
m_pIp(g_Config.m_SvSqlIp),
m_Port(g_Config.m_SvSqlPort)
{
	str_copy(m_aMap, g_Config.m_SvMap, sizeof(m_aMap));
	NormalizeMapname(m_aMap);
	
	if(gs_SqlLock == 0)
		gs_SqlLock = lock_create();
	
	Init();
}

CSqlScore::~CSqlScore()
{
	lock_wait(gs_SqlLock);
	lock_release(gs_SqlLock);
}

bool CSqlScore::Connect()
{
	try
	{
		// Create connection
		m_pDriver = get_driver_instance();
		char aBuf[256];
		str_format(aBuf, sizeof(aBuf), "tcp://%s:%d", m_pIp, m_Port);
		m_pConnection = m_pDriver->connect(aBuf, m_pUser, m_pPass);
		
		// Create Statement
		m_pStatement = m_pConnection->createStatement();
				
		// Create database if not exists
		str_format(aBuf, sizeof(aBuf), "CREATE DATABASE IF NOT EXISTS %s; ", m_pDatabase);
		m_pStatement->execute(aBuf);
		
		// Connect to specific database
		m_pConnection->setSchema(m_pDatabase);
		dbg_msg("SQL", "SQL connection established");
		return true;
	}
	catch (sql::SQLException &e)
	{
		char aBuf[256];
		str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
		dbg_msg("SQL", aBuf);
		
		dbg_msg("SQL", "ERROR: SQL connection failed");
		return false;
	}
	catch (const std::exception& ex) {
		// ...
		dbg_msg("SQL", "1 %s",ex.what());
		
	} catch (const std::string& ex) {
		// ...
		dbg_msg("SQL", "2 %s",ex.c_str());
	}
	catch( int )
	{
		dbg_msg("SQL", "3 %s");
	}
	catch( float )
	{
		dbg_msg("SQL", "4 %s");
	}
	
	catch( char[] )
	{
		dbg_msg("SQL", "5 %s");
	}
	
	catch( char )
	{
		dbg_msg("SQL", "6 %s");
	}
	catch (...)
	{
		dbg_msg("SQL", "Unknown Error cause by the MySQL/C++ Connector, my advice compile server_debug and use it");
		
		dbg_msg("SQL", "ERROR: SQL connection failed");
		return false;
	}
	return false;
}

void CSqlScore::Disconnect()
{
	try
	{
		delete m_pConnection;
		dbg_msg("SQL", "SQL connection disconnected");
	}
	catch (sql::SQLException &e)
	{
		dbg_msg("SQL", "ERROR: No SQL connection");
	}
}

char* CSqlScore::GetDBVersion()
{
	char aBuf[256];
	char *ddrace_version_db = (char *) malloc(32*sizeof(char));
	
	// Check if version >= 1.061a (thus we have a table called %s_version)
	str_format(aBuf, sizeof(aBuf), "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s_version';",m_pDDRaceTablesPrefix);
	m_pResults = m_pStatement->executeQuery(aBuf);
	
	if(m_pResults->rowsCount() < 1) // Version < 1.063a
		strcpy(ddrace_version_db,"0.5 trunk, 1.051a");
		// If less than 1.061a we have each map a single table. (or none if first sql use)
		// Each table has the naming convention record_MAPNAME_race 
		// Tables can be with or without timestamp
	else 
	{	
		// Get Version from DB
		str_format(aBuf, sizeof(aBuf), "SELECT version FROM %s_version LIMIT 0, 1;",m_pDDRaceTablesPrefix);
		m_pResults = m_pStatement->executeQuery(aBuf);
		
		if(m_pResults->next())
			strcpy(ddrace_version_db,m_pResults->getString("version").c_str());		
	}
	
	str_format(aBuf, sizeof(aBuf), "SQL ddrace database tables version detected: %s",ddrace_version_db);
	dbg_msg("SQL",aBuf);
	
	return ddrace_version_db;
}
void CSqlScore::UpdateDBVersion(char *pFromVersion){
	if(strcmp(pFromVersion,"0.5 trunk, 1.063a")<0)
	{	
		char aBuf[1024];
		
		dbg_msg("SQL", "Starting db conversion");
		std::string record_prefix = m_pPrefix + std::string("_");
		std::string record_suffix = "_race";	
		
		sql::Statement *statement2 = m_pConnection->createStatement();
		sql::ResultSet *results2 = NULL;
		
		sql::PreparedStatement *prepStatement = NULL;		
		
		// Must split following sql-commands otherwise I always get (no matter what I google and try) an 
		// MySQL Error: "Commands out of sync; you can't run this command now"
		// If someone can fix that problem, go ahead
		
		// create table: maps
		str_format(aBuf, sizeof(aBuf), 
				   "CREATE TABLE `%s_maps` ("
				   "`ID` int(4) NOT NULL AUTO_INCREMENT,"
				   "`Name` varchar(255) NOT NULL,"
				   "`IgnoreRunsBeforeMapCRCID` int(6) NOT NULL,"				   
				   "PRIMARY KEY (`ID`),KEY `Name` (`Name`)"
				   ") DEFAULT CHARSET=utf8 AUTO_INCREMENT=1;",m_pDDRaceTablesPrefix);
		m_pStatement->execute(aBuf);
		
		// create table: map_crcs
		str_format(aBuf, sizeof(aBuf), 
				   "CREATE TABLE `%s_map_crcs` ("
				   "`ID` int(6) NOT NULL AUTO_INCREMENT,"
				   "`MapID` varchar(255) NOT NULL,"
				   "`CRC` varchar(8) DEFAULT NULL,"
				   "`TimeAdded` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
				   "PRIMARY KEY (`ID`),KEY `MapID` (`MapID`),KEY `CRC` (`CRC`)"
				   ") DEFAULT CHARSET=utf8 AUTO_INCREMENT=1;",m_pDDRaceTablesPrefix);
		m_pStatement->execute(aBuf);		
		
		// create table: players
		str_format(aBuf, sizeof(aBuf),		
				   "CREATE TABLE `%s_players` ("
				   "`ID` int(9) NOT NULL AUTO_INCREMENT,"
				   "`Name` varchar(%d) NOT NULL,"
				   "`TimeAdded` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"				   
				   "PRIMARY KEY (`ID`), UNIQUE KEY `Name` (`Name`)"
				   ") DEFAULT CHARSET=utf8 AUTO_INCREMENT=1;",m_pDDRaceTablesPrefix,MAX_NAME_LENGTH);
		m_pStatement->execute(aBuf);
		
		// create table: runs
		str_format(aBuf, sizeof(aBuf),				
				   "CREATE TABLE `%s_runs` ("
				   "`ID` int(9) NOT NULL AUTO_INCREMENT,"
				   "`MapCRCID` int(4) NOT NULL,"
				   "`PlayerID` int(9) NOT NULL,"
				   "`Time` float NOT NULL DEFAULT '0',"
				   "`TimeOfEvent` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
				   "PRIMARY KEY (`ID`),KEY `MapCRCID` (`MapCRCID`),"
				   "KEY `PlayerID` (`PlayerID`)) AUTO_INCREMENT=1;", m_pDDRaceTablesPrefix);
		m_pStatement->execute(aBuf);		
		
		// create table: record_checkpoints
		str_format(aBuf, sizeof(aBuf),				
				   "CREATE TABLE `%s_record_checkpoints` ("
				   "`RunID` int(9) NOT NULL,"
				   "`Number` int(2) NOT NULL,"
				   "`Time` float DEFAULT '0',"
				   "UNIQUE KEY `RunID_2` (`RunID`,`Number`),"
				   "KEY `RunID` (`RunID`)"
				   ") DEFAULT CHARSET=utf8;",m_pDDRaceTablesPrefix);
		m_pStatement->execute(aBuf);		
		
		// create table: teams
		str_format(aBuf, sizeof(aBuf),				
				   "CREATE TABLE `%s_teams` ("
				   "`ID` int(6) NOT NULL AUTO_INCREMENT,"
				   "`Name` varchar(255) DEFAULT NULL,"
				   "`TimeAdded` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"				   
				   "PRIMARY KEY (`ID`), UNIQUE KEY `Name` (`Name`)"
				   ") DEFAULT CHARSET=utf8 AUTO_INCREMENT=1;",m_pDDRaceTablesPrefix);
		m_pStatement->execute(aBuf);	
		
		// create table: team_members
		str_format(aBuf, sizeof(aBuf),				
				   "CREATE TABLE `%s_team_members` ("
				   "`TeamID` int(6) NOT NULL,"				   
				   "`PlayerID` int(9) NOT NULL,"				   
				   "UNIQUE KEY `TeamMembersID` (`TeamID`,`PlayerID`),"
				   "KEY `PlayerID` (`PlayerID`)"
				   ") DEFAULT CHARSET=utf8;",m_pDDRaceTablesPrefix,MAX_NAME_LENGTH);
		m_pStatement->execute(aBuf);
		
		// create table: team_runs
		str_format(aBuf, sizeof(aBuf),				
				   "CREATE TABLE `%s_team_runs` ("
				   "`ID` int(9) NOT NULL AUTO_INCREMENT,"
				   "`MapCRCID` int(4) NOT NULL,"
				   "`TeamID` int(6) NOT NULL,"
				   "`Time` float NOT NULL DEFAULT '0',"
				   "`TimeOfEvent` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
				   "PRIMARY KEY (`ID`),KEY `MapCRCID` (`MapCRCID`),"
				   "KEY `TeamID` (`TeamID`)) AUTO_INCREMENT=1;", m_pDDRaceTablesPrefix);
		m_pStatement->execute(aBuf);			
		
		// create table: record_team_checkpoints
		str_format(aBuf, sizeof(aBuf),				
				   "CREATE TABLE `%s_team_record_checkpoints` ("
				   "`RunID` int(9) NOT NULL,"
				   "`Number` int(2) NOT NULL,"
				   "`Time` float DEFAULT '0',"
				   "UNIQUE KEY `RunID_2` (`RunID`,`Number`),"
				   "KEY `RunID` (`RunID`)"
				   ") DEFAULT CHARSET=utf8;",m_pDDRaceTablesPrefix);
		m_pStatement->execute(aBuf);		
		
		// create table: version
		str_format(aBuf, sizeof(aBuf),
				   "CREATE TABLE `%s_version` ("
				   "`version` varchar(32) NOT NULL,"
				   "UNIQUE KEY `version` (`version`)"
				   ")", m_pDDRaceTablesPrefix);
		m_pStatement->execute(aBuf);
		
		str_format(aBuf, sizeof(aBuf), "INSERT INTO `%s_version` (`version`) VALUES ('%s');",m_pDDRaceTablesPrefix,GAME_VERSION);
		m_pStatement->execute(aBuf);
		
		dbg_msg("SQL","Created new DB Structure. Starting with transformation and cleanup");	
		
		// Starting Transformation
		// Transfer all data into one big temp table
		
		// create temp table: temp_all_runs
		str_format(aBuf, sizeof(aBuf),				
				   "CREATE TABLE `%s_temp_all_runs` ("
				   "`RunID` int(9) NOT NULL AUTO_INCREMENT,"
				   "`PlayerName` VARCHAR(%d) NOT NULL,"
				   "`MapCRCID` int(4) NOT NULL,"					
				   "`Time` FLOAT DEFAULT 0,"
				   "`TimeOfEvent` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," 
				   "`cp1` float DEFAULT '0',"
				   "`cp2` float DEFAULT '0',"
				   "`cp3` float DEFAULT '0',"
				   "`cp4` float DEFAULT '0',"
				   "`cp5` float DEFAULT '0',"
				   "`cp6` float DEFAULT '0',"
				   "`cp7` float DEFAULT '0',"
				   "`cp8` float DEFAULT '0',"
				   "`cp9` float DEFAULT '0',"
				   "`cp10` float DEFAULT '0',"
				   "`cp11` float DEFAULT '0',"
				   "`cp12` float DEFAULT '0',"
				   "`cp13` float DEFAULT '0',"
				   "`cp14` float DEFAULT '0',"
				   "`cp15` float DEFAULT '0',"
				   "`cp16` float DEFAULT '0',"
				   "`cp17` float DEFAULT '0',"
				   "`cp18` float DEFAULT '0',"
				   "`cp19` float DEFAULT '0',"
				   "`cp20` float DEFAULT '0',"
				   "`cp21` float DEFAULT '0',"
				   "`cp22` float DEFAULT '0',"
				   "`cp23` float DEFAULT '0',"
				   "`cp24` float DEFAULT '0',"
				   "`cp25` float DEFAULT '0',"
				   "KEY `RunID` (`RunID`)"
				   ") DEFAULT CHARSET=utf8;",m_pDDRaceTablesPrefix,MAX_NAME_LENGTH);
		m_pStatement->execute(aBuf);		
		
		// Next we will go through all old tables with name like: %s_[Mapname]_race 
		// so later we can write all [Mapname]'s into table %s_map
		str_format(aBuf, sizeof(aBuf), "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' Group By TABLE_NAME",m_pDatabase);
		m_pResults = m_pStatement->executeQuery(aBuf);	
		
		int mapIDindex = 1;
		
		// Iterate threw all tables in our database
		while(m_pResults->next())
		{	
			std::string table_name = m_pResults->getString("TABLE_NAME").c_str();
			
			int comparePrefix = table_name.compare (0,	record_prefix.length(), record_prefix);
			int compareSuffix = table_name.compare (table_name.length() - record_suffix.length(),	record_suffix.length(), record_suffix);
			
			// If table matches %s_[Mapname]_race pattern ---> get it's data
			if (table_name.length() >= record_prefix.length() && table_name.length() >= record_suffix.length())
			{
				if(0 == comparePrefix && 0 == compareSuffix)
				{
					// Put Mapname from %s_[Mapname]_race into table %s_map
					std::string map_name = table_name.substr(record_prefix.length(),table_name.length()-record_prefix.length()-record_suffix.length());
					str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_maps(ID,Name,IgnoreRunsBeforeMapCRCID) VALUES ('%d','%s','%d');",m_pDDRaceTablesPrefix,mapIDindex,map_name.c_str(),mapIDindex);
					statement2->execute(aBuf);				
					str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_map_crcs(ID,MapID,TimeAdded) VALUES ('%d','%d',CURRENT_TIMESTAMP());",m_pDDRaceTablesPrefix,mapIDindex,mapIDindex);
					statement2->execute(aBuf);					
					
					// Get all entries from %s_[Mapname]_race table, except those where Name is empty
					str_format(aBuf, sizeof(aBuf), "SELECT * FROM %s_%s_race WHERE Name <> ''",m_pPrefix,map_name.c_str());
					results2 = statement2->executeQuery(aBuf);
					
					// Special case: old tables from ddrace version < 1.051a don't have a timestamp column
					int timestamp_available = results2->findColumn("Timestamp");
					char stringTimestamp[20] = "0000-00-00 00:00:00";
					
					while (results2->next()) {
						// If table contains Timestamp column, use it's value (might be 0000-00-00 00:00:00 as well though)
						if (timestamp_available != 0) {
							strcpy(stringTimestamp,results2->getString("Timestamp").c_str());
						}
						float cps[NUM_CHECKPOINTS];
						char cpText[5];
						for (int i=0; i<25; i++) {
							str_format(cpText, sizeof(cpText), "cp%d",i+1);
							cps[i] = (float)results2->getDouble(cpText);
							str_format(aBuf, sizeof(aBuf), "cp%d = %.2f",i+1,cps[i]);
						}
						// Now write all gathered data into our one big temp-table					
						str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_temp_all_runs (PlayerName, MapCRCID, TimeOfEvent, Time, cp1, cp2, cp3, cp4, cp5, cp6, cp7, cp8, cp9, cp10, cp11, cp12, cp13, cp14, cp15, cp16, cp17, cp18, cp19, cp20, cp21, cp22, cp23, cp24, cp25) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",m_pDDRaceTablesPrefix);		
						
						prepStatement = m_pConnection->prepareStatement(aBuf); // anti sql injection
						prepStatement->setString(1,results2->getString("Name"));
						prepStatement->setInt(2,mapIDindex);
						prepStatement->setDateTime(3,stringTimestamp);
						prepStatement->setDouble(4,results2->getDouble("Time"));
						for (int i=0; i<25; i++) {
							prepStatement->setDouble(5+i,cps[i]);
						}
						prepStatement->execute();
						
					}					
					mapIDindex++;
				}	
			}
		}		
		// Now we have all record_[Mapname]_race tables in our single temp-table 
		// Thus:
		// %s_temp_all_runs		complete
		// %s_maps				complete
		
		// Next fill %s_players
		// Group player names and put them into player table with above strategy
		str_format(aBuf, sizeof(aBuf), "SELECT PlayerName, Min(TimeOfEvent) as TimeAdded FROM %s_temp_all_runs Group By PlayerName",m_pDDRaceTablesPrefix);
		m_pResults = m_pStatement->executeQuery(aBuf);
		
		while (m_pResults->next()) {							
			str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_players(Name,TimeAdded) VALUES (?,?);",m_pDDRaceTablesPrefix);
			prepStatement = m_pConnection->prepareStatement(aBuf);
			prepStatement->setString(1,m_pResults->getString("PlayerName"));
			prepStatement->setDateTime(2,m_pResults->getString("TimeAdded"));			
			prepStatement->execute();
		}	
		// Now we have associated all player's (/their names) with an unique ID
		// Thus:
		// %s_players			complete		
		
		// Next we fill %s_runs
		// Wave new PlayerID into our big temp table results
		str_format(aBuf, sizeof(aBuf), "SELECT no2.ID as PlayerID, no1.* FROM %s_temp_all_runs as no1 LEFT JOIN %s_players as no2 ON no1.PlayerName = no2.Name",m_pDDRaceTablesPrefix,m_pDDRaceTablesPrefix);
		m_pResults = m_pStatement->executeQuery(aBuf);
		
		// Iterate threw all rows from our temp table with those PlayerID's, and 
		// write it's ID, Map, PlayerID, Time and TimeOfEvent 
		// into %s_runs
		while (m_pResults->next()) {		
			str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_runs(ID, MapCRCID, PlayerID, Time, TimeOfEvent) VALUES ('%d','%d','%d','%.2f','%s');",m_pDDRaceTablesPrefix,m_pResults->getInt("RunID"),m_pResults->getInt("MapCRCID"),m_pResults->getInt("PlayerID"),(float)m_pResults->getDouble("Time"),m_pResults->getString("TimeOfEvent").c_str());
			statement2->execute(aBuf); // TODO: Check for all getInt's what happens if int is out of range (when long would be needed)
		}		
		
		str_format(aBuf, sizeof(aBuf),
				   "SELECT TempRuns.* "
				   "FROM ("
				   "SELECT runs.ID AS RunID "
				   "FROM ("
				   "SELECT allRuns.* "
				   "FROM ("
				   "SELECT ID, MapCRCID, PlayerID, TIME, MIN( TIME ) AS minTime "
				   "FROM %s_runs "
				   "GROUP BY MapCRCID, PlayerID "
				   ") AS topRuns "
				   "LEFT JOIN %s_runs AS allRuns "
				   "ON topRuns.MapCRCID = allRuns.MapCRCID && topRuns.playerID = allRuns.playerID && topRuns.minTime = allRuns.time "
				   "GROUP BY MapCRCID, PlayerID, minTime "
				   ") AS runs "
				   "LEFT JOIN %s_players AS player ON player.ID = runs.PlayerID "
				   "ORDER BY MapCRCID "
				   ") AS RecordRunIDs "
				   "LEFT JOIN %s_temp_all_runs AS TempRuns ON RecordRunIDs.RunID = TempRuns.RunID "
				   "ORDER BY TIME",
				   m_pDDRaceTablesPrefix,m_pDDRaceTablesPrefix,m_pDDRaceTablesPrefix,m_pDDRaceTablesPrefix);
		m_pResults = m_pStatement->executeQuery(aBuf);
		while (m_pResults->next()) {
			char cpText[5];
			for (int i=0; i<25; i++) {
				str_format(cpText, sizeof(cpText), "cp%d",i+1);
				float time = (float)m_pResults->getDouble(cpText);
				if(time == 0) 
					continue; 
				// TODO: break could make more sense maybe, yet I have a map where you can 
				// skip a checkpoint cause of a circular map layout (each circle you get a
				// new weapon and thus a new way for the circle) 
				// but of course I could also rethink the map design, I keep continue for now		
				str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_record_checkpoints (RunID, Number, Time) VALUES (%d,%d,%.2f);",m_pDDRaceTablesPrefix,m_pResults->getInt("RunID"),i+1,time);	
				m_pStatement->execute(aBuf);
			}		
		}						
		//str_format(aBuf, sizeof(aBuf), "DROP TABLE IF EXISTS `%s_temp_all_runs`",m_pDDRaceTablesPrefix);
		//m_pStatement->execute(aBuf);
		
		delete prepStatement;		
		delete results2;
		delete statement2;
		dbg_msg("SQL", "Finished with db conversion");
	}
}

void CSqlScore::InitVarsFromDB()
{
	dbg_msg("SQL","Initializing server related sql vars");
	CServer* pServ = (CServer *)m_pServer;
	char pMapCrc[9];
	char aBuf[1024];
	str_format(pMapCrc, sizeof(pMapCrc), "%08x", pServ->m_CurrentMapCrc);
	int ignoreCRCsSmallerThan = 0;
	m_usedMapCRCIDs[0] = '\0';
	
	// 3 possibilties: 
	// 1. Map is completly new		= new mapID, new CRCID
	// 2. Map known but new CRC		= old mapID, new CRCID	
	// 3. Map known and CRC old		= old mapID, old CRCID
	
	// Check 1.: Map is completly new
	str_format(aBuf, sizeof(aBuf), "SELECT ID, IgnoreRunsBeforeMapCRCID FROM %s_maps WHERE Name = '%s' LIMIT 0, 1;", m_pDDRaceTablesPrefix, m_aMap);
	m_pResults = m_pStatement->executeQuery(aBuf);	
	if(m_pResults->rowsCount() < 1)
	{	// 1. is true: Map is completly new

		// Create new Map entry		
		str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_maps(ID,Name) VALUES (NULL,'%s');", m_pDDRaceTablesPrefix, m_aMap);
		m_pStatement->execute(aBuf);

		// and get that ID
		str_format(aBuf, sizeof(aBuf), "SELECT LAST_INSERT_ID() as ID;", m_pDDRaceTablesPrefix, m_aMap);
		m_pResults = m_pStatement->executeQuery(aBuf);
		while (m_pResults->next()) 
		{
			m_aMapSQLID = m_pResults->getInt("ID");
		}
	}
	else
	{	// 2. or 3. is true: Map is known
		m_pResults->next();
		m_aMapSQLID = m_pResults->getInt("ID");
		ignoreCRCsSmallerThan = m_pResults->getInt("IgnoreRunsBeforeMapCRCID"); // get old value
	}	
	// Now we have a map ID for sure, still don't know whats about the CRC

	// Check 2. vs. 3.: Map known but new CRC vs. Map is not new
	str_format(aBuf, sizeof(aBuf), "SELECT ID FROM %s_map_crcs WHERE MapID = '%d' AND CRC = '%s' LIMIT 0, 1;", m_pDDRaceTablesPrefix, m_aMapSQLID, pMapCrc);
	m_pResults = m_pStatement->executeQuery(aBuf);
	if(m_pResults->rowsCount() < 1)
	{	// 2. is true: Map known but new CRC
		
		// Create new CRC entry			
		str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_map_crcs(ID,MapID,CRC,TimeAdded) VALUES (NULL,'%d','%s',CURRENT_TIMESTAMP());", m_pDDRaceTablesPrefix, m_aMapSQLID, pMapCrc);
		m_pStatement->execute(aBuf);
		
		// and get that ID				
		str_format(aBuf, sizeof(aBuf), "SELECT LAST_INSERT_ID() AS ID;", m_pDDRaceTablesPrefix, m_aMap);
		m_pResults = m_pStatement->executeQuery(aBuf);
		while (m_pResults->next()) 
		{
			m_aMapCRCSQLID = m_pResults->getInt("ID");
		}	
		// TODO: Maybe give possibility to set this to newest CRC always
		if(ignoreCRCsSmallerThan == 0){			
			str_format(aBuf, sizeof(aBuf), "UPDATE %s_maps SET IgnoreRunsBeforeMapCRCID=LAST_INSERT_ID() WHERE ID = %d;", m_pDDRaceTablesPrefix, m_aMapSQLID, pMapCrc);
			m_pStatement->execute(aBuf);
			ignoreCRCsSmallerThan = m_aMapCRCSQLID;
		}
	}
	else
	{	// 3. is true: Map + CRC aren't new
		
		m_pResults->next();
		m_aMapCRCSQLID = m_pResults->getInt("ID");
	}					
	
	// Get CRC ID
	str_format(aBuf, sizeof(aBuf), 
		"SELECT crcs.ID as CRCID FROM "
		"(SELECT * FROM %s_maps WHERE ID = %d) as maps "
		"LEFT JOIN %s_map_crcs as crcs "
		"ON maps.ID = crcs.MapID "
		"WHERE crcs.ID >= maps.IgnoreRunsBeforeMapCRCID;", m_pDDRaceTablesPrefix, m_aMapSQLID, m_pDDRaceTablesPrefix);
	m_pResults = m_pStatement->executeQuery(aBuf);
	
	char pCRCIDasString[12];
	if(m_pResults->rowsCount()>0)
		while (m_pResults->next()) 
		{	
			str_format(pCRCIDasString,sizeof(pCRCIDasString),"%d", m_pResults->getInt("CRCID"));
			strcat(m_usedMapCRCIDs, pCRCIDasString);
			if(!m_pResults->isLast())
				strcat(m_usedMapCRCIDs,", ");
		}	
	// get the best time
	str_format(aBuf, sizeof(aBuf), "SELECT min(Time) as Time FROM `%s_runs` WHERE MapCRCID IN (%s)", m_pDDRaceTablesPrefix, m_usedMapCRCIDs);
	m_pResults = m_pStatement->executeQuery(aBuf);
	if(m_pResults->next())
	{
		((CGameControllerDDRace*)GameServer()->m_pController)->m_CurrentRecord = (float)m_pResults->getDouble("Time");
	}
	
	// get the best team time
	str_format(aBuf, sizeof(aBuf), "SELECT min(Time) as Time FROM `%s_team_runs` WHERE MapCRCID IN (%s)", m_pDDRaceTablesPrefix, m_usedMapCRCIDs);
	m_pResults = m_pStatement->executeQuery(aBuf);
	if(m_pResults->next())
	{
		((CGameControllerDDRace*)GameServer()->m_pController)->m_CurrentTeamRecord = (float)m_pResults->getDouble("Time");	
	}	
	dbg_msg("SQL", "Getting best time on server done");		
}

// create tables... should be done only once
void CSqlScore::Init()
{
	// create connection
	if(Connect())
	{
		try
		{
			// create tables or update db and get map data
			char *ddrace_version_db = GetDBVersion();
			UpdateDBVersion(ddrace_version_db); free(ddrace_version_db);
			InitVarsFromDB();			
			
			// delete results
			delete m_pResults;			
			
			// delete statement
			delete m_pStatement;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Tables were NOT created");
		}
		
		// disconnect from database
		Disconnect();
	}
}

// update stuff
void CSqlScore::LoadScoreThread(void *pUser)
{
	lock_wait(gs_SqlLock);
	
	CSqlScoreData *pData = (CSqlScoreData *)pUser;
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{			
			char aBuf[512];
			
			CPlayerData* playerData = (CPlayerData *)pData->m_pSqlData->PlayerData(pData->m_ClientID);
			
			// Check if the player has an entry in player table and get ID
			str_format(aBuf, sizeof(aBuf), "SELECT ID FROM %s_players WHERE Name = ? LIMIT 0, 1;", pData->m_pSqlData->m_pDDRaceTablesPrefix);
			sql::PreparedStatement *prepStatement = pData->m_pSqlData->m_pConnection->prepareStatement(aBuf);
			prepStatement->setString(1,pData->m_aName);
			pData->m_pSqlData->m_pResults = prepStatement->executeQuery();			

			// If not, create an entry in player table
			if(!pData->m_pSqlData->m_pResults->next() || pData->m_pSqlData->m_pResults->rowsCount() < 1)
			{
				str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_players(ID,Name) VALUES (NULL,?);", pData->m_pSqlData->m_pDDRaceTablesPrefix);
				prepStatement = pData->m_pSqlData->m_pConnection->prepareStatement(aBuf);
				prepStatement->setString(1,pData->m_aName);
				prepStatement->execute();
				delete prepStatement;

				str_format(aBuf, sizeof(aBuf), "SELECT LAST_INSERT_ID() as ID;", pData->m_pSqlData->m_pDDRaceTablesPrefix, pData->m_aName);
				pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);	
				pData->m_pSqlData->m_pResults->next();
			}
			
			// and get ID
			playerData->m_playerSQLID = pData->m_pSqlData->m_pResults->getInt("ID");

			str_format(aBuf, sizeof(aBuf), "Player %s has SQL ID %ld",pData->m_aName,playerData->m_playerSQLID);
			dbg_msg("SQL",aBuf); // TODO: remove if enough testet... id being wrong was a resistant bug
			
			// get best time and related checkpoints
			str_format(aBuf, sizeof(aBuf), 				   
					   "SELECT * FROM "
					   "(SELECT ID, Time as RunTime FROM %s_runs WHERE PlayerID = %ld AND MapCRCID IN (%s) "
					   "ORDER BY TIME ASC "
					   "LIMIT 0,1) as run "
					   "LEFT JOIN %s_record_checkpoints as recordCps "
					   "ON run.ID = recordCps.RunID;", pData->m_pSqlData->m_pDDRaceTablesPrefix, playerData->m_playerSQLID,pData->m_pSqlData->m_usedMapCRCIDs, pData->m_pSqlData->m_pDDRaceTablesPrefix);

			pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);

			dbg_msg("SQL", "Getting best time of player ");
			
			if (pData->m_pSqlData->m_pResults->next()) 
			{
				pData->m_pSqlData->PlayerData(pData->m_ClientID)->m_BestTime = (float)pData->m_pSqlData->m_pResults->getDouble("RunTime");
				pData->m_pSqlData->PlayerData(pData->m_ClientID)->m_CurrentTime = (float)pData->m_pSqlData->m_pResults->getDouble("RunTime");				
				// get the best time
				str_format(aBuf, sizeof(aBuf), "It's %.2f",(float)pData->m_pSqlData->m_pResults->getDouble("RunTime"));
				dbg_msg("SQL",aBuf);
				str_format(aBuf, sizeof(aBuf), "It's %.2f",pData->m_pSqlData->PlayerData(pData->m_ClientID)->m_BestTime);	
				dbg_msg("SQL",aBuf);
				
				// TODO: those two times differed sometimes (different rounding), check: still the case ?							
				do 
				{
					// get the checkpoint times				
					if(g_Config.m_SvCheckpointSave)
					{
						int i = pData->m_pSqlData->m_pResults->getInt("Number");
						float time = pData->m_pSqlData->m_pResults->getDouble("Time");					
						pData->m_pSqlData->PlayerData(pData->m_ClientID)->m_aBestCpTime[i] = time;
					}
				} while (pData->m_pSqlData->m_pResults->next());
			}
			dbg_msg("SQL", "Getting best time of player done");
			
			// force score reload if this was called as result of a name change f.e.
			pData->m_pSqlData->GameServer()->SendRecord(pData->m_ClientID);			
			
			// delete statement and results
			delete pData->m_pSqlData->m_pStatement;
			delete pData->m_pSqlData->m_pResults;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not update account");
		}
		
		// disconnect from database
		pData->m_pSqlData->Disconnect();
	}
	
	delete pData;

	lock_release(gs_SqlLock);
}

void CSqlScore::LoadScore(int ClientID)
{
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_ClientID = ClientID;
	str_copy(Tmp->m_aName, Server()->ClientName(ClientID), sizeof(Tmp->m_aName));
	Tmp->m_pSqlData = this;
	
	void *LoadThread = thread_create(LoadScoreThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)LoadThread);
#endif
}

void CSqlScore::LoadTeamScoreThread(void *_pData)
{
	lock_wait(gs_SqlLock);
	
	CSqlScoreData *pData = (CSqlScoreData *)_pData;
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{			
			char aBuf[512]; 
			char aNumberStringChain[512]; 			
			char aStringNumber[11];
			int pTeam = pData->m_pTeam;
			int pTeamsCount = pData->m_pTeams->Count(pTeam);
			CGameTeams *pTeams = pData->m_pTeams;
			int pPlayerSQLIDs[MAX_CLIENTS];
						
			char names[255];
			str_format(names, sizeof(names),"");
			
			for(int i = 0, j = 0; i < MAX_CLIENTS; ++i)
			{
				if(pTeam == pTeams->m_Core.Team(i))
				{
					CPlayerData *pPlayerData = pData->m_pSqlData->PlayerData(i);
					pPlayerSQLIDs[j] = pPlayerData->m_playerSQLID;	
					const char* playerName = pData->m_pSqlData->Server()->ClientName(i);
					str_format(aStringNumber,sizeof(aStringNumber),"%d",pPlayerData->m_playerSQLID);
					strcat(aNumberStringChain,aStringNumber);
					strcat(names,playerName);
					if (++j < pTeamsCount) {
						strcat(aNumberStringChain,", ");
						strcat(names,", ");	
					}
				}
			}
			
			
			// TODO: replace with this query
			//					SELECT ddrace_team_members.TeamID, COUNT(*) AS Members, SUM(ddrace_team_members.PlayerID IN(1, 5)) AS PlayersFromYourSetThatAreInThisTeam
			//					FROM ddrace_team_members
			//					JOIN
			//					(
			//					 SELECT DISTINCT ddrace_team_members.TeamID
			//					 FROM ddrace_team_members
			//					 WHERE ddrace_team_members.PlayerID IN(1, 5)
			//					 ) AS relevant_teams ON relevant_teams.TeamID = ddrace_team_members.TeamID
			//					GROUP BY ddrace_team_members.TeamID
			//					HAVING members = PlayersFromYourSetThatAreInThisTeam AND members = 2
			
			
			//			std::string pName = pData->m_pSqlData->Server()->ClientName();
			//			str_format(aBuf,sizeof(aBuf),"Player %s ID %d is one of %d players in Team %d",pName.c_str(),pChars[0]->GetPlayer()->GetCID(),pTeamsCount,pTeam);
			//			dbg_msg("SQL",aBuf);
			str_format(aBuf,sizeof(aBuf), 	
						"SELECT TeamID, COUNT(*) AS MemberCount, SUM(PlayerID IN(%s)) AS SumCount "
						"FROM %s_team_members "
						"GROUP BY TeamID "
						"HAVING MemberCount = SumCount AND MemberCount = %d "
					   , aNumberStringChain, pData->m_pSqlData->m_pDDRaceTablesPrefix, pTeamsCount
					   );
			
			pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);
			
			if (pData->m_pSqlData->m_pResults->rowsCount() == 1 && pData->m_pSqlData->m_pResults->next()) 
			{
				pData->m_pSqlData->TeamData(pTeam)->m_teamSQLID = (long)pData->m_pSqlData->m_pResults->getInt("TeamID");
				//pTeams->m_BestTime[pTeam] = (float)pData->m_pSqlData->m_pResults->getDouble("RunTime");
				
				
			}
			else
			{
				// Create new Team ID
				str_format(aBuf,sizeof(aBuf),"INSERT INTO %s_teams (ID,name) Values (NULL,?);",pData->m_pSqlData->m_pDDRaceTablesPrefix,names);
				sql::PreparedStatement *prepStatement = pData->m_pSqlData->m_pConnection->prepareStatement(aBuf);
				prepStatement->setString(1,names);
				prepStatement->execute();
				delete prepStatement;
				
				// Get new Team ID
				str_format(aBuf,sizeof(aBuf),"SELECT LAST_INSERT_ID() as ID;");
				pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);
				if (pData->m_pSqlData->m_pResults->rowsCount() == 1 && pData->m_pSqlData->m_pResults->next()) 
				{
					pData->m_pSqlData->TeamData(pTeam)->m_teamSQLID = (long)pData->m_pSqlData->m_pResults->getInt("ID");
				}
				else
				{
					//sth. is wrong (Allisone: how to throw exception ?)
//					pData->m_pSqlData->Disconnect();
					goto endTeamScoreLoad;
				}
				
				// Add Players to Team
				for(int i = 0; i < pTeamsCount; i++)
				{
					str_format(aBuf,sizeof(aBuf),"INSERT INTO %s_team_members (TeamID,PlayerID) Values (%d,%ld);", pData->m_pSqlData->m_pDDRaceTablesPrefix, pData->m_pSqlData->TeamData(pTeam)->m_teamSQLID, pPlayerSQLIDs[i]);
					pData->m_pSqlData->m_pStatement->execute(aBuf);		
				}
			}	
				
			// get best time and related checkpoints
			str_format(aBuf, sizeof(aBuf), 				   
					   "SELECT * FROM "
					   "(SELECT ID, Time as RunTime FROM %s_team_runs WHERE TeamID = %ld AND MapCRCID IN (%s) "
					   "ORDER BY TIME ASC "
					   "LIMIT 0,1) as run "
					   "LEFT JOIN %s_team_record_checkpoints as recordCps "
					   "ON run.ID = recordCps.RunID;", pData->m_pSqlData->m_pDDRaceTablesPrefix, pData->m_pSqlData->TeamData(pTeam)->m_teamSQLID,pData->m_pSqlData->m_usedMapCRCIDs, pData->m_pSqlData->m_pDDRaceTablesPrefix);

			pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);
			
			dbg_msg("SQL", "Getting best time of team ");
			
			if (pData->m_pSqlData->m_pResults->next()) 
			{
				//pTeams->SetBestTime(pTeam, (float)pData->m_pSqlData->m_pResults->getDouble("RunTime"));
								
//				pData->m_pSqlData->TeamData(pTeam)->m_BestTime = (pData->m_pSqlData->TeamData(pTeam)->m_BestTime)?pData->m_pSqlData->TeamData(pTeam)->m_BestTime:-9999;
//				pData->m_pSqlData->TeamData(pTeam)->m_CurrentTime = (pData->m_pSqlData->TeamData(pTeam)->m_CurrentTime)?pData->m_pSqlData->TeamData(pTeam)->m_CurrentTime:-9999;

				
				pData->m_pSqlData->TeamData(pTeam)->m_BestTime = (float)pData->m_pSqlData->m_pResults->getDouble("RunTime");
				pData->m_pSqlData->TeamData(pTeam)->m_CurrentTime = (float)pData->m_pSqlData->m_pResults->getDouble("RunTime");	
				pTeams->SendRecordToTeam(pTeam);	
				
				do 
				{
					// get the checkpoint times				
					if(g_Config.m_SvCheckpointSave)
					{
						int i = pData->m_pSqlData->m_pResults->getInt("Number");
						float time = pData->m_pSqlData->m_pResults->getDouble("Time");					
						pData->m_pSqlData->TeamData(pTeam)->m_aBestCpTime[i] = time;						
					}
				} while (pData->m_pSqlData->m_pResults->next());
			}
			else
			{
				pData->m_pSqlData->TeamData(pTeam)->m_BestTime = (float)0;
				pData->m_pSqlData->TeamData(pTeam)->m_CurrentTime = (float)0;
				pTeams->SendRecordToTeam(pTeam);
			}
			//pTeams->SetServerBestTime(pData->m_pSqlData->m_pTeamsRecordServer);
			dbg_msg("SQL", "Getting best time of team done");
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not update account");
		}
	endTeamScoreLoad:
		// disconnect from database
		pData->m_pSqlData->Disconnect();
	}
	
	delete pData;

	lock_release(gs_SqlLock);	
}

void CSqlScore::LoadTeamScore(int Team, CGameTeams *pTeams)
{	
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_pTeams = pTeams;	
	Tmp->m_pTeam = Team;
	Tmp->m_pSqlData = this;
	Tmp->m_pTeams = (CGameTeams *)pTeams;

	void *LoadThread = thread_create(LoadTeamScoreThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)LoadThread);
#endif	
}

void CSqlScore::SaveTeamScoreThread(void *_pData){
	lock_wait(gs_SqlLock);
	
	CSqlScoreData *pData = (CSqlScoreData *)_pData;
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{
			char aBuf[768];
			CTeamData* teamData = (CTeamData *)pData->m_pSqlData->TeamData(pData->m_pTeam);
			
			str_format(aBuf,sizeof(aBuf),"Saving Team score for team sql id = %ld, team no = %d",pData->m_pSQLID, pData->m_pTeam);
			pData->m_pSqlData->GameServer()->SendChatTarget(-1, aBuf);
			
			// get the old best time from db
			str_format(aBuf, sizeof(aBuf), 				   
					   "SELECT ID, Time FROM %s_team_runs WHERE TeamID = '%ld' AND MapCRCID IN (%s) "
					   "ORDER BY TIME ASC "
					   "LIMIT 0,1;", pData->m_pSqlData->m_pDDRaceTablesPrefix, pData->m_pSQLID,pData->m_pSqlData->m_usedMapCRCIDs, pData->m_pSqlData->m_pDDRaceTablesPrefix);
			
			pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);
			
			float oldBest = 0.0;
			int oldID = 0;
			if (pData->m_pSqlData->m_pResults->next()) {
				oldBest = (float)pData->m_pSqlData->m_pResults->getDouble("Time");
				oldID = pData->m_pSqlData->m_pResults->getInt("ID");
			}	
			
			// insert entry in runs
			str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_team_runs(ID, MapCRCID, TeamID, Time, TimeOfEvent) VALUES (NULL,'%d','%ld','%.2f', CURRENT_TIMESTAMP())",pData->m_pSqlData->m_pDDRaceTablesPrefix,pData->m_pSqlData->m_aMapCRCSQLID,pData->m_pSQLID,pData->m_Time);
			pData->m_pSqlData->m_pStatement->execute(aBuf);
			
			dbg_msg("SQL", "Adding new run time done");
			
			// if our new time is besser, save checkpoints
			if (oldBest == 0.0 || oldBest >= (float)pData->m_Time) {
				for (int i=0; i<25; i++) {
					float time = teamData->m_aBestCpTime[i];
					if(time == 0) 
						continue; 
					// TODO: break could make more sense maybe, yet I have a map where you can 
					// skip a checkpoint cause of a circular map layout (each circle you get a
					// new weapon and thus a new way for the circle) 
					// but of course I could also rethink the map design, I keep continue for now
					if (oldBest != 0.0) {
						// Update old record entries
						str_format(aBuf, sizeof(aBuf), "UPDATE %s_team_record_checkpoints SET RunID=LAST_INSERT_ID(), Number='%d',Time='%.2f' WHERE RunID=%ld AND Number=%d;",pData->m_pSqlData->m_pDDRaceTablesPrefix,i+1,time,oldID,i+1);
						if(pData->m_pSqlData->m_pStatement->executeUpdate(aBuf) > 0){
							continue; 
							// if we changed a row continue, 
							// but if not, execute insert query below
							// this happens if a map was updated and a new checkpoint was added, 
							// thus we can't update a previous checkpoint as it didn't exist yet in the previous run
						}
					}
					str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_team_record_checkpoints (RunID, Number, Time) VALUES (LAST_INSERT_ID(),'%d','%.2f');",pData->m_pSqlData->m_pDDRaceTablesPrefix,i+1,time);	
					pData->m_pSqlData->m_pStatement->execute(aBuf);				
				}
				dbg_msg("SQL", "Update of checkpoints done");				
			}
			// delete results statement
			delete pData->m_pSqlData->m_pStatement;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not update time");
		}
		
		// disconnect from database
		pData->m_pSqlData->Disconnect(); //TODO:Check if an exception is caught will this still execute ?
	}
	
	delete pData;
	
	lock_release(gs_SqlLock);
}

void CSqlScore::SaveTeamScore(int Team, float Time, CGameTeams *pTeams){
	CConsole* pCon = (CConsole*)GameServer()->Console();
	if(pCon->m_Cheated)
		return;
		
	if (TeamData(Team)->m_teamSQLID == 999999999) {
		dbg_msg("SQL","A Team ID was bogus");
		return;
	}
	
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_pTeams = pTeams;	
	Tmp->m_pTeam = Team;
	Tmp->m_Time = Time;
	Tmp->m_pSQLID = TeamData(Team)->m_teamSQLID;
	
	for(int i = 0; i < NUM_CHECKPOINTS; i++)
		Tmp->m_aCpCurrent[i] = pTeams->m_CpCurrent[Team][i];
	Tmp->m_pSqlData = this;
	
	void *SaveThread = thread_create(SaveTeamScoreThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)SaveThread);
#endif
}

void CSqlScore::SaveScoreThread(void *pUser)
{
	lock_wait(gs_SqlLock);
	
	CSqlScoreData *pData = (CSqlScoreData *)pUser;
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{
			char aBuf[768];
//			CPlayerData* playerData = (CPlayerData *)pData->m_pSqlData->PlayerData(pData->m_ClientID);
			
			// get the old best time from db
			str_format(aBuf, sizeof(aBuf), 				   
					   "SELECT ID, Time FROM %s_runs WHERE PlayerID = '%ld' AND MapCRCID IN (%s) "
					   "ORDER BY TIME ASC "
					   "LIMIT 0,1;", pData->m_pSqlData->m_pDDRaceTablesPrefix, pData->m_pSQLID,pData->m_pSqlData->m_usedMapCRCIDs, pData->m_pSqlData->m_pDDRaceTablesPrefix);
			
			pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);
			
			float oldBest = 0.0;
			int oldID = 0;
			if (pData->m_pSqlData->m_pResults->next()) {
				oldBest = (float)pData->m_pSqlData->m_pResults->getDouble("Time");
				oldID = pData->m_pSqlData->m_pResults->getInt("ID");
			}	
			
			// insert entry in runs
			str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_runs(ID, MapCRCID, PlayerID, Time, TimeOfEvent) VALUES (NULL,'%d','%ld','%.2f', CURRENT_TIMESTAMP())",pData->m_pSqlData->m_pDDRaceTablesPrefix,pData->m_pSqlData->m_aMapCRCSQLID,pData->m_pSQLID,pData->m_Time);
			pData->m_pSqlData->m_pStatement->execute(aBuf);
			
			dbg_msg("SQL", "Adding new run time done");
			
			// if our new time is besser, save checkpoints
			if (oldBest == 0.0 || oldBest >= (float)pData->m_Time) {
				for (int i=0; i<25; i++) {
					float time = pData->m_pSqlData->PlayerData(pData->m_ClientID)->m_aBestCpTime[i];
					if(time == 0) 
						continue; 
						// TODO: break could make more sense maybe, yet I have a map where you can 
						// skip a checkpoint cause of a circular map layout (each circle you get a
						// new weapon and thus a new way for the circle) 
						// but of course I could also rethink the map design, I keep continue for now
					if (oldBest != 0.0) {
						// Update old record entries
						str_format(aBuf, sizeof(aBuf), "UPDATE %s_record_checkpoints SET RunID=LAST_INSERT_ID(), Number='%d',Time='%.2f' WHERE RunID=%ld AND Number=%d;",pData->m_pSqlData->m_pDDRaceTablesPrefix,i+1,time,oldID,i+1);
						if(pData->m_pSqlData->m_pStatement->executeUpdate(aBuf) > 0){
							continue; 
							// if we changed a row continue, 
							// but if not, execute insert query below
							// this happens if a map was updated and a new checkpoint was added, 
							// thus we can't update a previous checkpoint as it didn't exist yet in the previous run
						}
					}
					str_format(aBuf, sizeof(aBuf), "INSERT INTO %s_record_checkpoints (RunID, Number, Time) VALUES (LAST_INSERT_ID(),'%d','%.2f');",pData->m_pSqlData->m_pDDRaceTablesPrefix,i+1,time);	
					pData->m_pSqlData->m_pStatement->execute(aBuf);				
				}
				dbg_msg("SQL", "Update of checkpoints done");				
			}
			// delete results statement
			delete pData->m_pSqlData->m_pStatement;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not update time");
		}
		
		// disconnect from database
		pData->m_pSqlData->Disconnect(); //TODO:Check if an exception is caught will this still execute ?
	}
	
	delete pData;
	
	lock_release(gs_SqlLock);
}

void CSqlScore::SaveScore(int ClientID, float Time, CCharacter *pChar)
{
	CConsole* pCon = (CConsole*)GameServer()->Console();
	if(pCon->m_Cheated)
		return;
	
	if (PlayerData(ClientID)->m_playerSQLID == 999999999) {
		dbg_msg("SQL", "Client SQL ID was bogus");
		return;
	}
	
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_ClientID = ClientID;
	Tmp->m_pSQLID = PlayerData(ClientID)->m_playerSQLID;	
	
	str_copy(Tmp->m_aName, Server()->ClientName(ClientID), sizeof(Tmp->m_aName));
	Tmp->m_Time = Time;
	for(int i = 0; i < NUM_CHECKPOINTS; i++)
		Tmp->m_aCpCurrent[i] = pChar->m_CpCurrent[i];
	Tmp->m_pSqlData = this;
	
	void *SaveThread = thread_create(SaveScoreThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)SaveThread);
#endif
}

void CSqlScore::ShowRankThread(void *pUser)
{
	lock_wait(gs_SqlLock);
	
	CSqlScoreData *pData = (CSqlScoreData *)pUser;
	//CSQLPlayerData* playerData = (CSQLPlayerData *)pData->m_pSqlData->PlayerData(pData->m_ClientID);	
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{
			sql::ResultSet *results = pData->m_pSqlData->queryIDsAndNamesIntoResults(pData);
		
			if(results)
			{
				char aBuf[800];
				char matchedName[MAX_NAME_LENGTH];					
				str_format(matchedName,sizeof(matchedName),"%s",results->getString("PlayerName").c_str());				
				int playerID = results->getInt("PlayerID");
				
				//CPlayerData* playerData = pData->m_pSqlData->PlayerData(pData->m_ClientID);
				
				if(g_Config.m_SvHideScore && playerID != pData->m_pSQLID){
					pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "You are not allowed to see other persons ranks on this server. Sorry.");
				}
				else{
					pData->m_pSqlData->m_pStatement->execute("SET @rownum := 0;");
					
					// Select Rank, PlayerID, Time, Ago-diff, TimeOfEvent stamp
					str_format(aBuf, sizeof(aBuf),
							   "SELECT Rank, minrun.PlayerID, minrun.Time, UNIX_TIMESTAMP( "
							   "CURRENT_TIMESTAMP ) - UNIX_TIMESTAMP( runs.TimeOfEvent ) AS Ago, UNIX_TIMESTAMP( runs.TimeOfEvent ) AS stamp "
							   "FROM ( "
							   "SELECT * "
							   "FROM ( "
							   "SELECT @rownum := @rownum +1 AS RANK, PlayerID, Time, MapCRCID "
							   "FROM ( "
							   "SELECT PlayerID, MIN( Time ) AS Time, MapCRCID "
							   "FROM %s_runs "
							   "WHERE MapCRCID IN (%s) "
							   "GROUP BY PlayerID "
							   ") AS all_top_times "
							   "ORDER BY TIME ASC "
							   ") AS all_ranks "
							   "WHERE all_ranks.PlayerID = %ld "
							   ") AS minrun "
							   "LEFT JOIN %s_runs AS runs ON runs.Time = minrun.Time && runs.PlayerID = minrun.PlayerID && runs.MapCRCID = minrun.MapCRCID "
							   "ORDER BY TimeOfEvent ASC "
							   "LIMIT 0 , 1"
							";", pData->m_pSqlData->m_pDDRaceTablesPrefix, pData->m_pSqlData->m_usedMapCRCIDs,playerID,pData->m_pSqlData->m_pDDRaceTablesPrefix);
					
					pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);
					
					// Check if we have a rank or not
					if(pData->m_pSqlData->m_pResults->rowsCount() != 1)
					{
						str_format(aBuf, sizeof(aBuf), "%s is not ranked", pData->m_aName);
						pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, aBuf);
					}
					else
					{
						// We have a rank, so we get the data and print it out
						pData->m_pSqlData->m_pResults->next();
						
						int since = (int)pData->m_pSqlData->m_pResults->getInt("Ago");
						char agoString[40];
						agoTimeToString(since,agoString);
						
						float Time = (float)pData->m_pSqlData->m_pResults->getDouble("Time");
						int Rank = (int)pData->m_pSqlData->m_pResults->getInt("Rank");
						
						if(playerID == pData->m_pSQLID && !pData->m_Search)
						{
							str_format(aBuf, sizeof(aBuf), "%d. Your time: %d minute(s) %5.2f second(s)", Rank, (int)(Time/60), Time-((int)Time/60*60));
							pData->m_pSqlData->GameServer()->SendChatTarget(-1, aBuf);
						}
						else if(!g_Config.m_SvHideScore)
						{
							str_format(aBuf, sizeof(aBuf), "%d. %s Time: %d minute(s) %5.2f second(s)", Rank, matchedName, (int)(Time/60), Time-((int)Time/60*60), agoString);
							pData->m_pSqlData->GameServer()->SendChat(-1, CGameContext::CHAT_ALL, aBuf);							
						}
						
						if(pData->m_pSqlData->m_pResults->getInt("stamp") != 0)
						{
							str_format(aBuf, sizeof(aBuf), "Finished: %s ago", agoString);
							if(!g_Config.m_SvHideScore)
							{
								strcat(aBuf, pData->m_aRequestingPlayer);
								pData->m_pSqlData->GameServer()->SendChat(-1, CGameContext::CHAT_ALL, aBuf);
							}
							else						
								pData->m_pSqlData->GameServer()->SendChatTarget(-1, aBuf);							
						}
					}
					dbg_msg("SQL", "Showing rank done");
					
					// delete results and statement
					delete pData->m_pSqlData->m_pResults;
					delete pData->m_pSqlData->m_pStatement;
				}
			}
			
			delete results;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not show rank");
		}
		
		// disconnect from database
		pData->m_pSqlData->Disconnect();//TODO:Check if an exception is caught will this still execute ?
	}
	
	delete pData;
	
	lock_release(gs_SqlLock);
}

void CSqlScore::ShowRank(int ClientID, const char* pName, bool Search)
{
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_ClientID = ClientID;
	str_copy(Tmp->m_aName, pName, sizeof(Tmp->m_aName));
	Tmp->m_Search = Search;
	Tmp->m_pSQLID = PlayerData(ClientID)->m_playerSQLID;
	str_format(Tmp->m_aRequestingPlayer, sizeof(Tmp->m_aRequestingPlayer), " (%s)", Server()->ClientName(ClientID));
	Tmp->m_pSqlData = this;
	
	void *RankThread = thread_create(ShowRankThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)RankThread);
#endif
}

void CSqlScore::ShowTop5Thread(void *pUser)
{
	lock_wait(gs_SqlLock);
	
	CSqlScoreData *pData = (CSqlScoreData *)pUser;
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{
			char aBuf[512];
			
			//
			// Player Top 5
			//
			str_format(aBuf, sizeof(aBuf), 
					   "SELECT players.Name, Time, Ago, Stamp FROM "
					   "(SELECT PlayerID, min(Time) as Time, UNIX_TIMESTAMP(CURRENT_TIMESTAMP)-UNIX_TIMESTAMP(TimeOfEvent) as Ago, UNIX_TIMESTAMP(TimeOfEvent) as Stamp "
					   "FROM %s_runs "
					   "WHERE MapCRCID IN (%s) "
					   "Group By PlayerID "
					   "ORDER BY `Time` ASC LIMIT %d, 5) as runs "
					   "LEFT JOIN %s_players as players "
					   "ON runs.PlayerID = players.ID;", pData->m_pSqlData->m_pDDRaceTablesPrefix, pData->m_pSqlData->m_usedMapCRCIDs, pData->m_Num-1,pData->m_pSqlData->m_pDDRaceTablesPrefix);
			pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);
			
			// show top5
			pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "----------- Top 5 -----------");
			
			int Rank = pData->m_Num;
			
			float pTime = 0;
			int pSince = 0;
			int pStamp = 0;
			while(pData->m_pSqlData->m_pResults->next())
			{
				char pAgoString[40] = "\0";
				pSince = (int)pData->m_pSqlData->m_pResults->getInt("Ago");
				pStamp = (int)pData->m_pSqlData->m_pResults->getInt("Stamp");
				pTime = (float)pData->m_pSqlData->m_pResults->getDouble("Time");
				
				agoTimeToString(pSince,pAgoString);
				
				if (pStamp == 0) {
					str_format(aBuf, sizeof(aBuf), "%d. %s: %d min %.2f sec", Rank, pData->m_pSqlData->m_pResults->getString("Name").c_str(), (int)(pTime/60), pTime-((int)pTime/60*60));
				}else{
					str_format(aBuf, sizeof(aBuf), "%d. %s: %d min %.2f sec, %s ago", Rank, pData->m_pSqlData->m_pResults->getString("Name").c_str(), (int)(pTime/60), pTime-((int)pTime/60*60), pAgoString);
				}

				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, aBuf);
				Rank++;
			}
			pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "--------------------------------");
			
			dbg_msg("SQL", "Showing top5 done");
			
			
			//
			// Team Top5
			//
			str_format(aBuf, sizeof(aBuf), 
					   "SELECT teams.Name, Time, Ago, Stamp FROM "
					   "(SELECT TeamID, min(Time) as Time, UNIX_TIMESTAMP(CURRENT_TIMESTAMP)-UNIX_TIMESTAMP(TimeOfEvent) as Ago, UNIX_TIMESTAMP(TimeOfEvent) as Stamp "
					   "FROM %s_team_runs "
					   "WHERE MapCRCID IN (%s) "
					   "Group By TeamID "
					   "ORDER BY `Time` ASC LIMIT %d, 5) as runs "
					   "LEFT JOIN %s_teams as teams "
					   "ON runs.TeamID = teams.ID;", pData->m_pSqlData->m_pDDRaceTablesPrefix, pData->m_pSqlData->m_usedMapCRCIDs, pData->m_Num-1,pData->m_pSqlData->m_pDDRaceTablesPrefix);
			pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);
			
			// show top5
			pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "--------- Team Top 5 ---------");
			
			Rank = pData->m_Num;
			
			pTime = 0;
			pSince = 0;
			pStamp = 0;
			while(pData->m_pSqlData->m_pResults->next())
			{
				char pAgoString[40] = "\0";
				pSince = (int)pData->m_pSqlData->m_pResults->getInt("Ago");
				pStamp = (int)pData->m_pSqlData->m_pResults->getInt("Stamp");
				pTime = (float)pData->m_pSqlData->m_pResults->getDouble("Time");
				
				agoTimeToString(pSince,pAgoString);
				
				if (pStamp == 0) {
					str_format(aBuf, sizeof(aBuf), "%d. %s: %d min %.2f sec", Rank, pData->m_pSqlData->m_pResults->getString("Name").c_str(), (int)(pTime/60), pTime-((int)pTime/60*60));
				}else{
					str_format(aBuf, sizeof(aBuf), "%d. %s: %d min %.2f sec, %s ago", Rank, pData->m_pSqlData->m_pResults->getString("Name").c_str(), (int)(pTime/60), pTime-((int)pTime/60*60), pAgoString);
				}
				
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, aBuf);
				Rank++;
			}
			pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "--------------------------------");
			
			dbg_msg("SQL", "Showing top5 done");
			
			
			// delete results and statement
			delete pData->m_pSqlData->m_pResults;
			delete pData->m_pSqlData->m_pStatement;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not show top5");
		}
		
		// disconnect from database
		pData->m_pSqlData->Disconnect();
	}
	
	delete pData;
	
	lock_release(gs_SqlLock);
}

void CSqlScore::ShowTimesThread(void *pUser)
{
	lock_wait(gs_SqlLock);
	CSqlScoreData *pData = (CSqlScoreData *)pUser;
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{
			char originalName[MAX_NAME_LENGTH];
			strcpy(originalName,pData->m_aName);
			pData->m_pSqlData->ClearString(pData->m_aName);
			
			char aBuf[512];
			char matchedName[MAX_NAME_LENGTH];			
				
			if(pData->m_Search) // last 5 times of a player
			{
				sql::ResultSet *pResults = pData->m_pSqlData->queryIDsAndNamesIntoResults(pData);
				if(pResults)
				{
					str_format(matchedName,sizeof(matchedName),"%s",pResults->getString("PlayerName").c_str());	
					int playerID = pResults->getInt("PlayerID");					
					
					str_format(aBuf, sizeof(aBuf), "SELECT Time, UNIX_TIMESTAMP(CURRENT_TIMESTAMP)-UNIX_TIMESTAMP(TimeOfEvent) as Ago, UNIX_TIMESTAMP(TimeOfEvent) as Stamp FROM %s_runs WHERE PlayerID = '%ld' ORDER BY Ago ASC LIMIT %d, 5;", pData->m_pSqlData->m_pDDRaceTablesPrefix, playerID, pData->m_Num-1);
					pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);
				}
				else{
					pData->m_pSqlData->m_pResults = NULL;
				}
				delete pResults;
			}
			else // last 5 times of server
			{
				str_format(aBuf, sizeof(aBuf), 
							"SELECT players.Name, times.* FROM "
							"(SELECT PlayerID, Time, UNIX_TIMESTAMP(CURRENT_TIMESTAMP)-UNIX_TIMESTAMP(TimeOfEvent) as Ago, UNIX_TIMESTAMP(TimeOfEvent) as Stamp "
							"FROM %s_runs "
							"WHERE MapCRCID in (%s) "
							"ORDER BY Ago ASC LIMIT %d, 5) as times "
							"JOIN %s_players as players "
							"ON times.PlayerID = players.ID;"
						   , pData->m_pSqlData->m_pDDRaceTablesPrefix, pData->m_pSqlData->m_usedMapCRCIDs, pData->m_Num-1, pData->m_pSqlData->m_pDDRaceTablesPrefix);
				pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);
			}
			
			if (pData->m_pSqlData->m_pResults == NULL) {
				// Player's times must have been searched but no player match have been found
				// All has been said, we don't need to do anything
			}
			else if(pData->m_pSqlData->m_pResults->rowsCount() == 0){
				// What ever has been requested, there are no results
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "There are no times in the specified range");	
			}
			else{
				if (pData->m_Search)
				{
					str_format(aBuf, sizeof(aBuf), "%s:",matchedName);					
					pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, aBuf);
				}				
				str_format(aBuf, sizeof(aBuf), "----------- Last times No %d - %d -----------",pData->m_Num,pData->m_Num + pData->m_pSqlData->m_pResults->rowsCount() - 1);
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, aBuf);
				
				float pTime = 0;
				int pSince = 0;
				int pStamp = 0;
				
				while(pData->m_pSqlData->m_pResults->next())
				{
					char pAgoString[40] = "\0";
					pSince = (int)pData->m_pSqlData->m_pResults->getInt("Ago");
					pStamp = (int)pData->m_pSqlData->m_pResults->getInt("Stamp");
					pTime = (float)pData->m_pSqlData->m_pResults->getDouble("Time");
					
					agoTimeToString(pSince,pAgoString);								
					
					if(pData->m_Search) // last 5 times of a player
					{
						if(pStamp == 0) // stamp is 00:00:00 cause it's an old entry from old times where there where no stamps yet
							str_format(aBuf, sizeof(aBuf), "%d min %.2f sec, don't know how long ago", (int)(pTime/60), pTime-((int)pTime/60*60));
						else					
							str_format(aBuf, sizeof(aBuf), "%d min %.2f sec, %s ago",(int)(pTime/60), pTime-((int)pTime/60*60),pAgoString);
					}
					else // last 5 times of the server
					{
						if(pStamp == 0) // stamp is 00:00:00 cause it's an old entry from old times where there where no stamps yet
							str_format(aBuf, sizeof(aBuf), "%s, %d m %.2f s, don't know when", pData->m_pSqlData->m_pResults->getString("Name").c_str(), (int)(pTime/60), pTime-((int)pTime/60*60));
						else					
							str_format(aBuf, sizeof(aBuf), "%s, %d m %.2f s, %s ago", pData->m_pSqlData->m_pResults->getString("Name").c_str(), (int)(pTime/60), pTime-((int)pTime/60*60), pAgoString);
					}
					pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, aBuf);
				}
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "--------------------------------------------------");
				
				dbg_msg("SQL", "Showing times done");
			}
			// delete results and statement
			delete pData->m_pSqlData->m_pResults;
			delete pData->m_pSqlData->m_pStatement;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not show times");
		}
		// disconnect from database
		pData->m_pSqlData->Disconnect();
	}
	delete pData;
	
	lock_release(gs_SqlLock);
}

void CSqlScore::ShowMapCRCsThread(void *pUser)
{
	lock_wait(gs_SqlLock);
	CSqlScoreData *pData = (CSqlScoreData *)pUser;
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{
			char aBuf[512];	
			
			pData->m_pSqlData->m_pStatement->execute("SET @rownum := -1;");
			
			str_format(aBuf, sizeof(aBuf), 
				"SELECT @rownum := @rownum + 1 as Number, crcs.*, maps.Name FROM "
				"(SELECT * FROM %s_map_crcs "
				"WHERE MapID = %d) as crcs "
				"LEFT JOIN %s_maps as maps "
				"ON crcs.MapID = maps.ID "
				"ORDER BY TimeAdded DESC", 
				pData->m_pSqlData->m_pDDRaceTablesPrefix, pData->m_pSqlData->m_aMapSQLID, pData->m_pSqlData->m_pDDRaceTablesPrefix);
			pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);					
				
			if(pData->m_pSqlData->m_pResults->rowsCount() > 0) // last 5 times of a player
			{
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "--------- Map CRCs -------");
				while(pData->m_pSqlData->m_pResults->next())
				{
					int Number = (int)pData->m_pSqlData->m_pResults->getInt("Number");
					std::string CRC = (strcmp(pData->m_pSqlData->m_pResults->getString("CRC").c_str(),"") != 0) ? pData->m_pSqlData->m_pResults->getString("CRC").c_str() : "had no crc yet";
					std::string Date = pData->m_pSqlData->m_pResults->getString("TimeAdded").c_str();
					std::string Name = pData->m_pSqlData->m_pResults->getString("Name").c_str();
					str_format(aBuf, sizeof(aBuf), "%d. %s %s, first used: %s",Number, Name.c_str(), CRC.c_str(), Date.c_str());
					pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, aBuf);
				}
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "--------------------------");				
			}
			else
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "This is a strange bug, please note what you have done and contact Trust at ddrace.info");
					
			dbg_msg("SQL", "Changing IgnoreRunsBeforeMapCRCID in maps table done");
		
			// delete results and statement
			delete pData->m_pSqlData->m_pResults;
			delete pData->m_pSqlData->m_pStatement;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not show times");
		}
		// disconnect from database
		pData->m_pSqlData->Disconnect();
	}
	delete pData;
	
	lock_release(gs_SqlLock);	
}

void CSqlScore::ShowTeamNameThread(void *_pData)
{
	lock_wait(gs_SqlLock);

	CSqlScoreData *pData = (CSqlScoreData *)_pData;

	int pTeamNo = pData->m_pTeam;
	//char* aName = pData->m_aName;
	//CGameTeams *pTeams = &((CGameControllerDDRace*)pData->m_pSqlData->GameServer()->m_pController)->m_Teams;
	long pTeamSQLID = pData->m_pSQLID;
		
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{
			char aBuf[512];	
			
			str_format(aBuf, sizeof(aBuf), 
					   "SELECT Name FROM %s_teams WHERE ID = %ld LIMIT %d,1", 
					   pData->m_pSqlData->m_pDDRaceTablesPrefix, pTeamSQLID);
			pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);					
			
			if(pData->m_pSqlData->m_pResults->rowsCount() == 1)
			{
				pData->m_pSqlData->m_pResults->next();
				str_format(aBuf, sizeof(aBuf),"Your team name: %s",pData->m_pSqlData->m_pResults->getString("Name").c_str());	
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, aBuf);
			}
			else
			{
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "Wow, you have tricked the system. Tell Trust at DDRace.info");
				str_format(aBuf,sizeof(aBuf),"Team No = %d ,SQL ID = %ld",pTeamNo,pTeamSQLID);
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, aBuf);				
			}
			
			// delete results and statement
			delete pData->m_pSqlData->m_pResults;
			delete pData->m_pSqlData->m_pStatement;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not show times");
		}
		// disconnect from database
		pData->m_pSqlData->Disconnect();
	}
	delete pData;
	
	lock_release(gs_SqlLock);	
}
void CSqlScore::SetTeamNameThread(void *_pData)
{
	lock_wait(gs_SqlLock);
	CSqlScoreData *pData = (CSqlScoreData *)_pData;
	//int pTeamNo = pData->m_pTeam;
	char* aName = pData->m_aName;
	//CGameTeams *pTeams = &((CGameControllerDDRace*)pData->m_pSqlData->GameServer()->m_pController)->m_Teams;
	long pTeamSQLID = pData->m_pSQLID;
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{
			char aBuf[512];	
			
			str_format(aBuf, sizeof(aBuf), 
					   "UPDATE %s_teams SET Name = '%s' WHERE ID = %ld;", 
					   pData->m_pSqlData->m_pDDRaceTablesPrefix, aName, pTeamSQLID);
			pData->m_pSqlData->m_pStatement->execute(aBuf);					
			
			pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "Team name changed");
			
			// delete results and statement
			delete pData->m_pSqlData->m_pStatement;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not show times");
		}
		// disconnect from database
		pData->m_pSqlData->Disconnect();
	}
	delete pData;
	
	lock_release(gs_SqlLock);	
}

void CSqlScore::IgnoreOldRunsThread(void *pUser)
{
	lock_wait(gs_SqlLock);
	CSqlScoreData *pData = (CSqlScoreData *)pUser;
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{
			char aBuf[512];	
			
			str_format(aBuf, sizeof(aBuf), 
				"SELECT ID as MapCRCID FROM %s_map_crcs WHERE MapID = %d ORDER BY TimeAdded DESC LIMIT %d,1", 
				pData->m_pSqlData->m_pDDRaceTablesPrefix, pData->m_pSqlData->m_aMapSQLID, pData->m_Num);
			pData->m_pSqlData->m_pResults = pData->m_pSqlData->m_pStatement->executeQuery(aBuf);					
				
			if(pData->m_pSqlData->m_pResults->rowsCount() == 1)
			{
				pData->m_pSqlData->m_pResults->next();
				int pMapCRCID = (int)pData->m_pSqlData->m_pResults->getInt("MapCRCID");	
				
				str_format(aBuf, sizeof(aBuf), 
					"UPDATE %s_maps SET IgnoreRunsBeforeMapCRCID = %d WHERE ID = %d;",
					pData->m_pSqlData->m_pDDRaceTablesPrefix, pMapCRCID, pData->m_pSqlData->m_aMapSQLID);
				pData->m_pSqlData->m_pStatement->execute(aBuf);	
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "Database was updated. Please reload the map now (/reload)");		
			}
			else
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "There are no maps crcs so far back. Type /mapCRCs to see a list of map versions.");
					
			dbg_msg("SQL", "Changing IgnoreRunsBeforeMapCRCID in maps table done");
		
			// delete results and statement
			delete pData->m_pSqlData->m_pResults;
			delete pData->m_pSqlData->m_pStatement;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not show times");
		}
		// disconnect from database
		pData->m_pSqlData->Disconnect();
	}
	delete pData;
	
	lock_release(gs_SqlLock);
}

void CSqlScore::IgnoreOldRunsByCRCThread(void *pUser)
{
	lock_wait(gs_SqlLock);
	CSqlScoreData *pData = (CSqlScoreData *)pUser;
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{
			char aBuf[512];	
			
			str_format(aBuf, sizeof(aBuf), 
				"SELECT ID as MapCRCID FROM %s_map_crcs WHERE MapID = %d AND CRC = ? LIMIT 0,1", 
				pData->m_pSqlData->m_pDDRaceTablesPrefix,pData->m_pSqlData->m_aMapSQLID);
				
			sql::PreparedStatement *prepStatement = pData->m_pSqlData->m_pConnection->prepareStatement(aBuf);
			prepStatement->setString(1,pData->m_aName);
			pData->m_pSqlData->m_pResults = prepStatement->executeQuery();
			delete prepStatement;					
				
			if(pData->m_pSqlData->m_pResults->rowsCount() == 1)
			{
				pData->m_pSqlData->m_pResults->next();
				int pMapCRCID = (int)pData->m_pSqlData->m_pResults->getInt("MapCRCID");	
				
				str_format(aBuf, sizeof(aBuf), 
					"UPDATE %s_maps SET IgnoreRunsBeforeMapCRCID = %d WHERE ID = %d;",
					pData->m_pSqlData->m_pDDRaceTablesPrefix, pMapCRCID, pData->m_pSqlData->m_aMapSQLID);
				pData->m_pSqlData->m_pStatement->execute(aBuf);	
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "Database was updated. Please reload the map now (/reload)");		
			}
			else
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "There is no map with such a crc. Type /mapCRCs to see a list of map versions.");
					
			dbg_msg("SQL", "Changing IgnoreRunsBeforeMapCRCID in maps table done");
		
			// delete results and statement
			delete pData->m_pSqlData->m_pResults;
			delete pData->m_pSqlData->m_pStatement;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not show times");
		}
		// disconnect from database
		pData->m_pSqlData->Disconnect();
	}
	delete pData;
	
	lock_release(gs_SqlLock);
}

void CSqlScore::IgnoreOldRunsByDateThread(void *pUser)
{
	lock_wait(gs_SqlLock);
	CSqlScoreData *pData = (CSqlScoreData *)pUser;
	
	// Connect to database
	if(pData->m_pSqlData->Connect())
	{
		try
		{
			char aBuf[512];	
			
			str_format(aBuf, sizeof(aBuf), 
				"SELECT ID as MapCRCID FROM %s_map_crcs WHERE MapID = %d AND TimeAdded >= ? ORDER BY TimeAdded ASC LIMIT 0,1", 
				pData->m_pSqlData->m_pDDRaceTablesPrefix,pData->m_pSqlData->m_aMapSQLID);
				
			sql::PreparedStatement *prepStatement = pData->m_pSqlData->m_pConnection->prepareStatement(aBuf);
			prepStatement->setString(1,pData->m_aName);
			pData->m_pSqlData->m_pResults = prepStatement->executeQuery();
			delete prepStatement;					
				
			if(pData->m_pSqlData->m_pResults->rowsCount() == 1)
			{
				pData->m_pSqlData->m_pResults->next();
				int pMapCRCID = (int)pData->m_pSqlData->m_pResults->getInt("MapCRCID");	
				
				str_format(aBuf, sizeof(aBuf), 
					"UPDATE %s_maps SET IgnoreRunsBeforeMapCRCID = %d WHERE ID = %d;",
					pData->m_pSqlData->m_pDDRaceTablesPrefix, pMapCRCID, pData->m_pSqlData->m_aMapSQLID);
				pData->m_pSqlData->m_pStatement->execute(aBuf);	
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "Database was updated. Please reload the map now (/reload)");		
			}
			else
				pData->m_pSqlData->GameServer()->SendChatTarget(pData->m_ClientID, "There is no map with such a crc. Type /mapCRCs to see a list of map versions.");
					
			dbg_msg("SQL", "Changing IgnoreRunsBeforeMapCRCID in maps table done");
		
			// delete results and statement
			delete pData->m_pSqlData->m_pResults;
			delete pData->m_pSqlData->m_pStatement;
		}
		catch (sql::SQLException &e)
		{
			char aBuf[256];
			str_format(aBuf, sizeof(aBuf), "MySQL Error: %s", e.what());
			dbg_msg("SQL", aBuf);
			dbg_msg("SQL", "ERROR: Could not show times");
		}
		// disconnect from database
		pData->m_pSqlData->Disconnect();
	}
	delete pData;
	
	lock_release(gs_SqlLock);
}

void CSqlScore::ShowTop5(int ClientID, int Debut)
{
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_Num = Debut;
	Tmp->m_ClientID = ClientID;
	Tmp->m_pSqlData = this;
	
	void *Top5Thread = thread_create(ShowTop5Thread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)Top5Thread);
#endif
}


void CSqlScore::ShowTimes(int ClientID, int Debut)
{
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_Num = Debut;
	Tmp->m_ClientID = ClientID;
	Tmp->m_pSqlData = this;
	Tmp->m_Search = false;
	
	void *TimesThread = thread_create(ShowTimesThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)TimesThread);
#endif	
}

void CSqlScore::ShowTimes(int ClientID, const char* pName, int Debut)
{
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_Num = Debut;
	Tmp->m_ClientID = ClientID;
	str_copy(Tmp->m_aName, pName, sizeof(Tmp->m_aName));
	Tmp->m_pSqlData = this;
	Tmp->m_Search = true;
	
	void *TimesThread = thread_create(ShowTimesThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)TimesThread);
#endif	
}

void CSqlScore::ShowMapCRCs(int ClientID)
{
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_ClientID = ClientID;
	Tmp->m_pSqlData = this;

	void *aThread = thread_create(ShowMapCRCsThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)aThread);
#endif
}
void CSqlScore::ShowTeamName(int CallerClientID, int TeamNo)
{
	if (TeamData(TeamNo)->m_teamSQLID == 999999999) 
	{
		GameServer()->SendChatTarget(CallerClientID, "You need to pass the start line first");
		return;
	}
	
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_ClientID = CallerClientID;
	Tmp->m_pTeam = TeamNo;
	Tmp->m_pSQLID = TeamData(TeamNo)->m_teamSQLID;
	Tmp->m_pSqlData = this;
	
	void *aThread = thread_create(ShowTeamNameThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)aThread);
#endif
}
void CSqlScore::SetTeamName(int CallerClientID, int TeamNo, const char* pName)
{
	if (TeamData(TeamNo)->m_teamSQLID == 999999999) 
	{
		GameServer()->SendChatTarget(CallerClientID, "You need to pass the start line first");
		return;
	}	
	
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_ClientID = CallerClientID;
	Tmp->m_pTeam = TeamNo;
	Tmp->m_pSQLID = TeamData(TeamNo)->m_teamSQLID;	
	str_copy(Tmp->m_aName, pName, sizeof(Tmp->m_aName));
	Tmp->m_pSqlData = this;
	
	void *aThread = thread_create(SetTeamNameThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)aThread);
#endif	
}
void CSqlScore::IgnoreOldRuns(int ClientID, int Before)
{
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_Num = Before;
	Tmp->m_ClientID = ClientID;
	Tmp->m_pSqlData = this;

	void *aThread = thread_create(IgnoreOldRunsThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)aThread);
#endif	
}
void CSqlScore::IgnoreOldRunsByCRC(int ClientID, const char* aCRC)
{
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_ClientID = ClientID;
	Tmp->m_pSqlData = this;
	str_copy(Tmp->m_aName, aCRC, sizeof(Tmp->m_aName));
	
	void *aThread = thread_create(IgnoreOldRunsByCRCThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)aThread);
#endif
}
void CSqlScore::IgnoreOldRunsByDate(int ClientID, const char* aDate)
{
	CSqlScoreData *Tmp = new CSqlScoreData();
	Tmp->m_ClientID = ClientID;
	Tmp->m_pSqlData = this;
	str_copy(Tmp->m_aName, aDate, sizeof(Tmp->m_aName));
	
	void *aThread = thread_create(IgnoreOldRunsByDateThread, Tmp);
#if defined(CONF_FAMILY_UNIX)
	pthread_detach((pthread_t)aThread);
#endif
}

// anti SQL injection

void CSqlScore::ClearString(char *pString)
{
	char newString[MAX_NAME_LENGTH*2-1];
	int pos = 0;
	
	for(int i=0;i<str_length(pString);i++) {
		if(pString[i] == '\\') {
			newString[pos++] = '\\';
			newString[pos++] = '\\';
		} else if(pString[i] == '\'') {
			newString[pos++] = '\\';
			newString[pos++] = '\'';
		} else if(pString[i] == '"') {
			newString[pos++] = '\\';
			newString[pos++] = '"';
		} else {
			newString[pos++] = pString[i];
		}
	}
	
	newString[pos] = '\0';
	
	strcpy(pString,newString);
}

void CSqlScore::NormalizeMapname(char *pString) 
{
	std::string validChars("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_");
	
	for(int i=0;i<str_length(pString);i++) {
		if(validChars.find(pString[i]) == std::string::npos) {
			pString[i] = '_';
		}
	}
}

sql::ResultSet *CSqlScore::queryIDsAndNamesIntoResults(void *pUser) 
{
	CSqlScoreData *pData = (CSqlScoreData *)pUser;
	char aBuf[800];
	char aBuf2[MAX_NAME_LENGTH+4];
	
	sql::ResultSet *results;
	
	str_format(aBuf, sizeof(aBuf),"I will search for a player with name like %s who finished this map",pData->m_aName);
	dbg_msg("SQL",aBuf);
	
	// Get PlayerID's and PlayerName's from this map out of playernames that contain our searchname
	str_format(aBuf, sizeof(aBuf),"SELECT * FROM "
			   "(SELECT Name as PlayerName, ID as PlayerID "
			   "FROM %s_players "
			   "WHERE ID IN ("
			   "SELECT ID FROM %s_players WHERE Name LIKE ? ORDER BY Length(Name)) "
			   ") as l "
			   "JOIN "
			   "(SELECT * FROM %s_runs WHERE MapCRCID IN (%s)) as r "
			   "ON l.PlayerID = r.PlayerID Group By PlayerName;",pData->m_pSqlData->m_pDDRaceTablesPrefix,pData->m_pSqlData->m_pDDRaceTablesPrefix,pData->m_pSqlData->m_pDDRaceTablesPrefix,pData->m_pSqlData->m_usedMapCRCIDs);

	// Use preparedStatement for anti SQL injection
	sql::PreparedStatement *prepStatement = pData->m_pSqlData->m_pConnection->prepareStatement(aBuf);
	str_format(aBuf2, sizeof(aBuf2),"%%%s%%",pData->m_aName);
	prepStatement->setString(1,aBuf2);
	results = prepStatement->executeQuery();
	delete prepStatement;
	
	results->next();
	
	// Now check if we have 0, 1, or more hits
	if(results->rowsCount() == 0){
		// 0 hits
		str_format(aBuf, sizeof(aBuf),"Could not find records for a name like %s",pData->m_aName);
		pData->m_pSqlData->GameServer()->SendChatTarget(-1, aBuf);	
		delete results;
		return NULL;
		
	}			
	else if(results->rowsCount() > 1)
	{			
		// 1++ hits
		if (strcmp(results->getString("PlayerName").c_str(),pData->m_aName)==0)
		{			
			// First row == search string
			dbg_msg("SQL","Found Name");
			return results;
		}
		else
		{
			// First row != search string							
			str_format(aBuf, sizeof(aBuf),"Please be more specific... more names were found that match: ");
			pData->m_pSqlData->GameServer()->SendChatTarget(-1, aBuf);	
			str_format(aBuf, sizeof(aBuf),"");
			do
			{
				strcat(aBuf,results->getString("PlayerName").c_str());
				dbg_msg("SQL",aBuf);
				if (!results->isLast())
				{
					strcat(aBuf,", ");
					// TODO: Once I got Name, [nothing anymore]
					// why ?
				}
			}while(results->next());
			pData->m_pSqlData->GameServer()->SendChatTarget(-1, aBuf);
			delete results;
			return NULL;
		}	
	}
	return results;
}

void CSqlScore::agoTimeToString(int agoTime, char agoString[]){
	char aBuf[20];
	int times[7] = {
		60 * 60 * 24 * 365 ,
		60 * 60 * 24 * 30 ,
		60 * 60 * 24 * 7,
		60 * 60 * 24 ,
		60 * 60 ,
		60 ,
		1
	};
	char names[7][6] = {
		"year",
		"month",
		"week",
		"day",
		"hour",
		"min",
		"sec"
	};
	
	int seconds = 0;
	char name[6];
	int count = 0;
	int i = 0;
	
	// finding biggest match
	for(i = 0; i<7; i++){
		seconds = times[i];
		strcpy(name,names[i]);
		
		count = floor((float)agoTime/(float)seconds);
		if(count != 0){
			break;
		}
	}
	str_format(agoString, sizeof(agoString), "");
	if(count == 1){
		str_format(aBuf, sizeof(aBuf), "%d %s", 1 , name);
	}else{
		str_format(aBuf, sizeof(aBuf), "%d %ss", count , name);
	}
	strcat(agoString,aBuf);
	
	if (i + 1 < 7) {
		// getting second piece now
		int seconds2 = times[i+1];
		char name2[6];
		strcpy(name2,names[i+1]);
		
		// add second piece if it's greater than 0
		int count2 = floor((float)(agoTime - (seconds * count)) / (float)seconds2);
		
		if (count2 != 0) {
			if(count2 == 1){
				str_format(aBuf, sizeof(aBuf), " and %d %s", 1 , name2);
			}else{
				str_format(aBuf, sizeof(aBuf), " and %d %ss", count2 , name2);
			}
			strcat(agoString,aBuf);
		}
	}
}
#endif
