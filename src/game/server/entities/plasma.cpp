/* copyright (c) 2007 magnus auvinen, see licence.txt for more info */
#include <engine/server.h>
#include <engine/config.h>
#include <game/generated/protocol.h>
#include <game/server/gamecontext.h>
#include "plasma.h"

const float ACCEL=1.1f;


//////////////////////////////////////////////////
// turret
//////////////////////////////////////////////////
CPlasma::CPlasma(CGameWorld *pGameWorld, vec2 Pos, vec2 Dir, int Freeze, bool Explosive)
: CEntity(pGameWorld, NETOBJTYPE_LASER)
{
	m_Pos = Pos;
	m_Core = Dir;
	m_Freeze = Freeze;
	m_Explosive = Explosive;
	m_EvalTick = Server()->Tick();
	m_LifeTime = Server()->TickSpeed() * 1.5;
	GameWorld()->InsertEntity(this);
}

bool CPlasma::HitCharacter()
{
	vec2 To2;
	CCharacter *Hit = GameServer()->m_World.IntersectCharacter(m_Pos, m_Pos+m_Core, 0.0f,To2);
	if(!Hit)
		return false;
	if(m_Freeze== -1)
		Hit->UnFreeze();
	else if (m_Freeze)
		Hit->Freeze(Server()->TickSpeed()*3);
	if(!m_Freeze || (m_Freeze && m_Explosive))
		GameServer()->CreateExplosion(m_Pos, -1, WEAPON_GRENADE, true);
	GameServer()->m_World.DestroyEntity(this);
	return true;
}

void CPlasma::Move()
{
	m_Pos += m_Core;
	m_Core *= ACCEL;
}
	
void CPlasma::Reset()
{
	GameServer()->m_World.DestroyEntity(this);
}

void CPlasma::Tick()
{
	if (m_LifeTime==0)
	{
		Reset();
		return;
	}
	m_LifeTime--;
	Move();
	HitCharacter();

	int Res=0;
	Res = GameServer()->Collision()->IntersectNoLaser(m_Pos, m_Pos+m_Core,0, 0);
	if(Res)
	{
		if(m_Explosive)
			GameServer()->CreateExplosion(m_Pos, -1, WEAPON_GRENADE, true);
		Reset();
	}
	
}

void CPlasma::Snap(int SnappingClient)
{	
	if(NetworkClipped(SnappingClient))
		return;

	CNetObj_Laser *pObj = static_cast<CNetObj_Laser *>(Server()->SnapNewItem(NETOBJTYPE_LASER, m_Id, sizeof(CNetObj_Laser)));
	pObj->m_X = (int)m_Pos.x;
	pObj->m_Y = (int)m_Pos.y;
	pObj->m_FromX = (int)m_Pos.x;
	pObj->m_FromY = (int)m_Pos.y;
	pObj->m_StartTick = m_EvalTick;
}