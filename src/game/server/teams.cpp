#include "teams.h"
#include <engine/shared/config.h>
#include <engine/server/server.h>
#include <cstring>

#if defined(CONF_SQL)
#include "score.h"
#include "score/sql_score.h"
#endif

#if defined(CONF_SQL)
#include "score.h"
#include "score/sql_score.h"
#endif

CGameTeams::CGameTeams(CGameContext *pGameContext) : m_pGameContext(pGameContext)
{
	Reset();
}

void CGameTeams::Reset()
{
	m_Core.Reset();
	for(int i = 0; i < MAX_CLIENTS; ++i)
	{
		m_TeamState[i] = TEAMSTATE_EMPTY;
		m_TeeFinished[i] = false;
		m_MembersCount[i] = 0;
		m_LastChat[i] = 0;
		m_BestTime[i] = 0.0;
		
		for(int j = 0; j < 25; j++){
			m_CheckPoints[i][j] = 0.0;
		}
	}
}

void CGameTeams::OnCharacterStart(int ClientID)
{
	int Tick = Server()->Tick();
	CCharacter* pStartingChar = Character(ClientID);
	if(!pStartingChar)
		return;
	if(pStartingChar->m_DDRaceState == DDRACE_FINISHED)
		pStartingChar->m_DDRaceState = DDRACE_NONE;
	if(m_Core.Team(ClientID) == TEAM_FLOCK || m_Core.Team(ClientID) == TEAM_SUPER)
	{
		pStartingChar->m_DDRaceState = DDRACE_STARTED;
		pStartingChar->m_StartTime = Tick;
		pStartingChar->m_RefreshTime = Tick;
	}
	else
	{
		bool Waiting = false;
		for(int i = 0; i < MAX_CLIENTS; ++i)
		{
			if(m_Core.Team(ClientID) == m_Core.Team(i))
			{
				CCharacter* pChar = Character(i);
				if(pChar->m_DDRaceState == DDRACE_FINISHED)
				{
					Waiting = true;
					if(m_LastChat[ClientID] + Server()->TickSpeed() + g_Config.m_SvChatDelay < Tick)
					{
						char aBuf[128];
						str_format(aBuf, sizeof(aBuf), "%s has finished and didn't go through start yet, wait for him or join another team.", Server()->ClientName(i));
						GameServer()->SendChatTarget(ClientID, aBuf);
						m_LastChat[ClientID] = Tick;
					}
					if(m_LastChat[i] + Server()->TickSpeed() + g_Config.m_SvChatDelay < Tick)
					{
						char aBuf[128];
						str_format(aBuf, sizeof(aBuf), "%s wants to start a new round, kill or walk to start.", Server()->ClientName(ClientID));
						GameServer()->SendChatTarget(i, aBuf);
						m_LastChat[i] = Tick;
					}
				}
			}
		}

		if(m_TeamState[m_Core.Team(ClientID)] <= TEAMSTATE_CLOSED && !Waiting)
		{
			CCharacter **pChars = (CCharacter **) malloc(Count(m_Core.Team(ClientID))*sizeof(CCharacter)); 
			// TODO: Allisone: have recently learned that we shouldn't use malloc. Will rebuild that.

			ChangeTeamState(m_Core.Team(ClientID), TEAMSTATE_STARTED);
			for(int i = 0, j = 0; i < MAX_CLIENTS; ++i)
			{
				if(m_Core.Team(ClientID) == m_Core.Team(i))
				{
					CCharacter* pChar = Character(i);
					if(pChar)
					{
						pChar->m_DDRaceState = DDRACE_STARTED;
						pChar->m_StartTime = Tick;
						pChar->m_RefreshTime = Tick;
						pChars[j]=pChar; 
						char aBuf[100];
						std::string pName = Server()->ClientName(pChars[j]->GetPlayer()->GetCID());						
						str_format(aBuf,sizeof(aBuf),"%d Name = %s",j,pName.c_str());
						dbg_msg("For SQL Team Score",aBuf);	
						j++;
					}
				}
			}
			GameServer()->Score()->LoadTeamScore(m_Core.Team(ClientID), pChars, this);
			// for(int i = 0; i < Count(Team); i++)
			// {
			// 	if(GameServer()->m_apPlayers[i] && pChars[i]->GetPlayer()->m_IsUsingDDRaceClient)
			// 	{
			// 		CNetMsg_Sv_Record RecordsMsg;
			// 		RecordsMsg.m_PlayerTimeBest = m_BestTime[Team] * 100.0f;
			// 		RecordsMsg.m_ServerTimeBest = GameServer()->m_pController->m_CurrentRecord * 100.0f;
			// 		Server()->SendPackMsg(&RecordsMsg, MSGFLAG_VITAL, i);
			// 	}
			// }
			// THIS IS NOW in SendTeamTimes	and should be called from within LoadTeamScoreThread			
		}
	}
}

void CGameTeams::OnCharacterFinish(int ClientID)
{
	if(m_Core.Team(ClientID) == TEAM_FLOCK || m_Core.Team(ClientID) == TEAM_SUPER)
	{
		Character(ClientID)->OnFinish();
	}
	else
	{
		m_TeeFinished[ClientID] = true;
		if(TeamFinished(m_Core.Team(ClientID)))
		{
			ChangeTeamState(m_Core.Team(ClientID), TEAMSTATE_OPEN);
			CCharacter *pChars[Count(m_Core.Team(ClientID))];
			for(int i = 0, j = 0; i < MAX_CLIENTS; ++i)
			{
				if(m_Core.Team(ClientID) == m_Core.Team(i))
				{
					CCharacter * pChar = Character(i);
					if(pChar != 0)
					{
						pChars[j]=pChar;
						j++;						

						m_TeeFinished[i] = false;
					}
				}
			}
#if defined(CONF_SQL)
			if (g_Config.m_SvUseSQL) {
				OnTeamFinish(m_Core.Team(ClientID), pChars);				
			}
#endif
		}
	}
}

void CGameTeams::OnTeamFinish(int Team, CCharacter *pChars[])
{
	char aBuf[500];
	float time = 0.0;
	CCharacter *pOneChar;
	pOneChar = pChars[0];		

	time =  (float)(Server()->Tick() - pOneChar->m_StartTime) / ((float)Server()->TickSpeed());

	if(time < 0.000001f) return;
//	CPlayerData *pData = GameServer()->Score()->PlayerData(m_pPlayer->GetCID());

	char names[255];
	str_format(names, sizeof(names),"");	

	for (int i = 0; i<Count(Team); i++) {
		CCharacter *pChar = pChars[i];
		int playerID = pChar->GetPlayer()->GetCID();
		const char* playerName = Server()->ClientName(playerID);
		
		pChar->m_DDRaceState = DDRACE_FINISHED;	
		pChar->m_CpActive=-2;
		
		if(pChar->GetPlayer()->m_IsUsingDDRaceClient) {
			CNetMsg_Sv_DDRaceTime Msg;
			Msg.m_Time = (int)(time * 100.0f);
			Msg.m_Check = 0;
			Msg.m_Finish = 1;
			
			if(m_BestTime[Team])
			{
				float Diff = (time - m_BestTime[Team])*100;
				Msg.m_Check = (int)Diff;
			}
			
			Server()->SendPackMsg(&Msg, MSGFLAG_VITAL, pChar->GetPlayer()->GetCID());
		}
		
		int TTime = 0-(int)time;
		if(pChar->GetPlayer()->m_Score < TTime)
			pChar->GetPlayer()->m_Score = TTime; 		
		
		strcat(names,playerName);
		
		if (i+1<Count(Team))
			strcat(names,", ");			
	}
	// Chat Inform about time in general
	if(g_Config.m_SvHideScore){
		str_format(aBuf, sizeof(aBuf), "Your team finished in: %d minute(s) %5.2f second(s)", (int)time/60, time-((int)time/60*60));
		GameServer()->SendChat(-1, Team, aBuf);
	}
	else{
		str_format(aBuf, sizeof(aBuf), "Team %s finished in: %d minute(s) %5.2f second(s)", names, (int)time/60, time-((int)time/60*60));
		GameServer()->SendChat(-1, CGameContext::CHAT_ALL, aBuf);		
	}
	
	// Chat Inform about relation of time to old time
	if(time < m_BestTime[Team])
	{
		// new record \o/
		str_format(aBuf, sizeof(aBuf), "New record: %5.2f second(s) better.", fabs(time - m_BestTime[Team]));
		if(g_Config.m_SvHideScore)
			GameServer()->SendChat(-1, Team, aBuf);
		else
			GameServer()->SendChat(-1, CGameContext::CHAT_ALL, aBuf);
	}
	else if(m_BestTime[Team] != 0) // tee has already finished?
	{
		if(fabs(time - m_BestTime[Team]) <= 0.005)
		{
			GameServer()->SendChat(-1, Team, "Your team finished with your best time.");
		}
		else
		{
			str_format(aBuf, sizeof(aBuf), "%5.2f second(s) worse, better luck next time.", fabs(m_BestTime[Team] - time));
			GameServer()->SendChat(-1, Team, aBuf);//this is private, sent only to the tee
		}
	}

	bool pCallSaveScore = false;
	bool NeedToSendNewRecord = false;	
#if defined(CONF_SQL)
	pCallSaveScore = g_Config.m_SvUseSQL;
#endif
	
	if(!m_BestTime[Team] || time < m_BestTime[Team])
	{
		// update player score	
		m_BestTime[Team] = time;
		for (int i=0; i<25; i++) {
			m_CheckPoints[Team][i] = pOneChar->m_CpCurrent[i];
		}
		pCallSaveScore = true;
		
		NeedToSendNewRecord = true;
//		for(int i = 0; i < MAX_CLIENTS; i++)
//		{
//			if(GameServer()->m_apPlayers[i] && GameServer()->m_apPlayers[i]->m_IsUsingDDRaceClient)
//			{
//				if(!g_Config.m_SvHideScore || i == pOneChar->GetPlayer()->GetCID())
//				{
//					CNetMsg_Sv_PlayerTime Msg;
//					Msg.m_Time = time * 100.0;
//					Msg.m_ClientID = pOneChar->GetPlayer()->GetCID();
//					Server()->SendPackMsg(&Msg, MSGFLAG_VITAL, i);
//				}
//			}
//		}
		
	}
	
	// send the run data to the score engine 
	if(pCallSaveScore)
		GameServer()->Score()->SaveTeamScore(Team, pChars, time, this);		

	
	// update server best time
	if(GameServer()->m_pController->m_CurrentRecord == 0 || time < GameServer()->m_pController->m_CurrentRecord)
	{
		GameServer()->m_pController->m_CurrentRecord = time;
		NeedToSendNewRecord = true;
	}
	
//	Scrap note:	CPlayerData *pData = GameServer()->Score()->PlayerData(m_pPlayer->GetCID());
	
	if(NeedToSendNewRecord) { 
		for(int i = 0; i < MAX_CLIENTS; i++)
		{
			if(GameServer()->m_apPlayers[i] && GameServer()->m_apPlayers[i]->m_IsUsingDDRaceClient)
			{
				CNetMsg_Sv_Record RecordsMsg;
				RecordsMsg.m_PlayerTimeBest = time * 100.0f;
				RecordsMsg.m_ServerTimeBest = GameServer()->m_pController->m_CurrentRecord * 100.0f;
				Server()->SendPackMsg(&RecordsMsg, MSGFLAG_VITAL, i);
			}
		}
	}
}

bool CGameTeams::SetCharacterTeam(int ClientID, int Team)
{
	//Check on wrong parameters. +1 for TEAM_SUPER
	if(ClientID < 0 || ClientID >= MAX_CLIENTS || Team < 0 || Team >= MAX_CLIENTS + 1)
		return false;
	//You can join to TEAM_SUPER at any time, but any other group you cannot if it started
	if(Team != TEAM_SUPER && m_TeamState[Team] >= TEAMSTATE_CLOSED)
		return false;
	//No need to switch team if you there
	if(m_Core.Team(ClientID) == Team)
		return false;
	//You cannot be in TEAM_SUPER if you not super
	if(Team == TEAM_SUPER && !Character(ClientID)->m_Super) return false;
	//if you begin race
	if(Character(ClientID)->m_DDRaceState != DDRACE_NONE)
	{
		//you will be killed if you try to join FLOCK
		if(Team == TEAM_FLOCK && m_Core.Team(ClientID) != TEAM_FLOCK)
			Character(ClientID)->GetPlayer()->KillCharacter(WEAPON_GAME);
		else if(Team != TEAM_SUPER)
			return false;
	}
	SetForceCharacterTeam(ClientID, Team);
	
	
	//GameServer()->CreatePlayerSpawn(Character(id)->m_Core.m_Pos, TeamMask());
	return true;
}

void CGameTeams::SetForceCharacterTeam(int ClientID, int Team)
{
	m_TeeFinished[ClientID] = false;
	if(m_Core.Team(ClientID) != TEAM_FLOCK 
		&& m_Core.Team(ClientID) != TEAM_SUPER 
		&& m_TeamState[m_Core.Team(ClientID)] != TEAMSTATE_EMPTY)
	{
		bool NoOneInOldTeam = true;
		for(int i = 0; i < MAX_CLIENTS; ++i)
			if(i != ClientID && m_Core.Team(ClientID) == m_Core.Team(i))
			{
				NoOneInOldTeam = false;//all good exists someone in old team
				break;
			} 
		if(NoOneInOldTeam)
			m_TeamState[m_Core.Team(ClientID)] = TEAMSTATE_EMPTY;
	}
	if(Count(m_Core.Team(ClientID)) > 0) m_MembersCount[m_Core.Team(ClientID)]--;
	m_Core.Team(ClientID, Team);
	if(m_Core.Team(ClientID) != TEAM_SUPER) m_MembersCount[m_Core.Team(ClientID)]++;
	if(Team != TEAM_SUPER && m_TeamState[Team] == TEAMSTATE_EMPTY)
		ChangeTeamState(Team, TEAMSTATE_OPEN);
	dbg_msg1("Teams", "Id = %d Team = %d", ClientID, Team);
	
	for (int LoopClientID = 0; LoopClientID < MAX_CLIENTS; ++LoopClientID)
	{
		if(Character(LoopClientID) && Character(LoopClientID)->GetPlayer()->m_IsUsingDDRaceClient)
			SendTeamsState(LoopClientID);
	}
}

int CGameTeams::Count(int Team) const
{
	if(Team == TEAM_SUPER)
		return -1;
	return m_MembersCount[Team];
}

void CGameTeams::ChangeTeamState(int Team, int State)
{
	m_TeamState[Team] = State;
}

bool CGameTeams::TeamFinished(int Team)
{
	for(int i = 0; i < MAX_CLIENTS; ++i)
		if(m_Core.Team(i) == Team && !m_TeeFinished[i])
			return false;
	return true;
}

int CGameTeams::TeamMask(int Team, int ExceptID)
{
	if(Team == TEAM_SUPER) return -1;
	int Mask = 0;
	for(int i = 0; i < MAX_CLIENTS; ++i)
		if(i != ExceptID)
			if((Character(i) && (m_Core.Team(i) == Team || m_Core.Team(i) == TEAM_SUPER))
				|| (GameServer()->m_apPlayers[i] && GameServer()->m_apPlayers[i]->GetTeam() == -1))
				Mask |= 1 << i;
	return Mask;
}

void CGameTeams::SendTeamsState(int ClientID)
{
	CNetMsg_Cl_TeamsState Msg;
	Msg.m_Tee0 = m_Core.Team(0);
	Msg.m_Tee1 = m_Core.Team(1);
	Msg.m_Tee2 = m_Core.Team(2);
	Msg.m_Tee3 = m_Core.Team(3);
	Msg.m_Tee4 = m_Core.Team(4);
	Msg.m_Tee5 = m_Core.Team(5);
	Msg.m_Tee6 = m_Core.Team(6);
	Msg.m_Tee7 = m_Core.Team(7);
	Msg.m_Tee8 = m_Core.Team(8);
	Msg.m_Tee9 = m_Core.Team(9);
	Msg.m_Tee10 = m_Core.Team(10);
	Msg.m_Tee11 = m_Core.Team(11);
	Msg.m_Tee12 = m_Core.Team(12);
	Msg.m_Tee13 = m_Core.Team(13);
	Msg.m_Tee14 = m_Core.Team(14);
	Msg.m_Tee15 = m_Core.Team(15);
	
	Server()->SendPackMsg(&Msg, MSGFLAG_VITAL, ClientID);	
}

void CGameTeams::SendTeamTimes(int Team, CCharacter **_pChars)
{
	CCharacter **pChars = (CCharacter **)(_pChars);
	for(int i = 0; i < Count(Team); i++)
	{
		if(pChars[i]->GetPlayer()->m_IsUsingDDRaceClient)
		{
			CNetMsg_Sv_Record RecordsMsg;
			RecordsMsg.m_PlayerTimeBest = m_BestTime[Team] * 100.0f;
			RecordsMsg.m_ServerTimeBest = GameServer()->m_pController->m_CurrentRecord * 100.0f;
			Server()->SendPackMsg(&RecordsMsg, MSGFLAG_VITAL, pChars[i]->GetPlayer()->GetCID());
		}
	}
}
