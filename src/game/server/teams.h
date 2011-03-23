#ifndef GAME_SERVER_TEAMS_H
#define GAME_SERVER_TEAMS_H

#include <game/teamscore.h>
#include <game/server/gamecontext.h>

class CGameTeams
{
	int m_TeamState[MAX_CLIENTS];
	int m_MembersCount[MAX_CLIENTS];
	bool m_TeeFinished[MAX_CLIENTS];

	float m_StartTime[MAX_CLIENTS];	
	float m_ServerBestTime;

	class CGameContext * m_pGameContext;
	
public:
	enum
	{
		TEAMSTATE_EMPTY, 
		TEAMSTATE_OPEN,
		TEAMSTATE_CLOSED,
		TEAMSTATE_STARTED,
		TEAMSTATE_FINISHED
	};
	float m_CpCurrent[MAX_CLIENTS][25]; // TODO: make private	
	
	CTeamsCore m_Core;
	
	CGameTeams(CGameContext *pGameContext);
	
	//helper methods
	CCharacter* Character(int ClientID) { return GameServer()->GetPlayerChar(ClientID); }

	class CGameContext *GameServer() { return m_pGameContext; }
	class IServer *Server() { return m_pGameContext->Server(); }
	
	void OnCharacterStart(int ClientID);
	void OnCharacterFinish(int ClientID);
	
	void OnTeamStarted(int Team);

	void OnTeamFinish(int Team);

	bool SetCharacterTeam(int ClientID, int Team);
	
	void SetServerBestTime(float Time) {m_ServerBestTime = Time;};	
	
	int GetServerBestTime() {return m_ServerBestTime;};	

	void ChangeTeamState(int Team, int State);
	
	bool TeamFinished(int Team);
	
	int TeamMask(int Team, int ExceptID = -1);
	
	int Count(int Team) const;
	
	//need to be very carefull using this method
	void SetForceCharacterTeam(int id, int Team);
	void CharacterDied(int id);
	
	void Reset();

	void SendTeamsState(int Cid);
	void SendTeamTimes(int Team);	

	int m_LastChat[MAX_CLIENTS];
};

#endif
