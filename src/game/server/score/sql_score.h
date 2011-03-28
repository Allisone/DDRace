/* CSqlScore Class by Sushi Tee*/
#ifndef GAME_SERVER_SQLSCORE_H
#define GAME_SERVER_SQLSCORE_H

#include <mysql_connection.h>

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>

#include "../score.h"

class CSqlScore : public IScore
{	
	CGameContext *m_pGameServer;
	IServer *m_pServer;

	sql::Driver *m_pDriver;
	sql::Connection *m_pConnection;
	sql::Statement *m_pStatement;
	sql::ResultSet *m_pResults;

	// copy of config vars
	const char* m_pDatabase;
	const char* m_pPrefix;
	const char* m_pDDRaceTablesPrefix;
	const char* m_pUser;
	const char* m_pPass;
	const char* m_pIp;
	int m_Port;
	char m_aMap[64];
	int m_aMapSQLID;
	int m_aMapCRCSQLID;	
	char m_usedMapCRCIDs[256]; // space for 36 IDs with length 6
	float m_pTeamsRecordServer;

	CGameContext *GameServer() { return m_pGameServer; }
	IServer *Server() { return m_pServer; }

	static void LoadScoreThread(void *pUser);
	static void LoadTeamScoreThread(void *pData);	
	static void SaveScoreThread(void *pUser);	
	static void SaveTeamScoreThread(void *pData);
	static void ShowRankThread(void *pUser);
	static void ShowTop5Thread(void *pUser);
	static void ShowTimesThread(void *pUser);
	static void ShowMapCRCsThread(void *pUser);	
	static void ShowTeamNameThread(void *pData);
	static void SetTeamNameThread(void *pData);	
	static void IgnoreOldRunsThread(void *pUser);
	static void IgnoreOldRunsByCRCThread(void *pUser);
	static void IgnoreOldRunsByDateThread(void *pUser);		

	void Init();
	char* GetDBVersion();
	void UpdateDBVersion(char *pFromVersion);
	void InitVarsFromDB();	

	bool Connect();
	void Disconnect();

	// anti SQL injection
	void ClearString(char *pString);

	void NormalizeMapname(char *pString);
	virtual sql::ResultSet *queryIDsAndNamesIntoResults(void *pUser);	

public:

	CSqlScore(CGameContext *pGameServer);
	~CSqlScore();
	
	virtual void LoadScore(int ClientID);
	virtual void LoadTeamScore(int Team, CGameTeams *pTeams);	
	virtual void SaveScore(int ClientID, float Time, CCharacter *pChar);
	virtual void SaveTeamScore(int Team, float Time, CGameTeams *pTeams);
	virtual void ShowRank(int ClientID, const char* pName, bool Search=false);
	virtual void ShowTop5(int ClientID, int Debut=1);
	virtual void ShowTimes(int ClientID, const char* pName, int Debut=1);
	virtual void ShowTimes(int ClientID, int Debut=1);
	virtual void ShowMapCRCs(int ClientID);
	virtual void ShowTeamName(int CallerClientID, int TeamNo);
	virtual void SetTeamName(int CallerClientID, int TeamNo, const char* pName);
	virtual void IgnoreOldRuns(int ClientID, int Before=0);
	virtual void IgnoreOldRunsByCRC(int ClientID, const char* aCRC);
	virtual void IgnoreOldRunsByDate(int ClientID, const char* aDate);		
 	static void agoTimeToString(int agoTime, char agoStrign[]);
};

struct CSqlScoreData
{
	CSqlScore *m_pSqlData;
	int m_ClientID;
/*
#if defined(CONF_FAMILY_WINDOWS)
	char m_aName[16]; // Don't edit this, or all your teeth will fall http://bugs.mysql.com/bug.php?id=50046
#else
*/
// edited :D ... nothing in this script would work with current windows drivers, that's why I will commit new drivers soon and compile description
// cause this ain't a bug but a feautre. Reason = Microsofts Visual Studio CRT. Drivers MUST be compiled with same CRT as Teeworlds. That's it.
// http://ddrace.info/forum/showthread.php?t=537

	char m_aName[MAX_NAME_LENGTH*2-1];
	float m_Time;
	float m_aCpCurrent[NUM_CHECKPOINTS];
	int m_Num;
	bool m_Search;
	char m_aRequestingPlayer[MAX_NAME_LENGTH];
	int m_pTeam;

	CGameTeams *m_pTeams;
};

#endif
