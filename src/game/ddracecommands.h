/* (c) Shereef Marzouk. See "licence DDRace.txt" and the readme.txt in the root of the distribution for more information. */
#ifndef GAME_SERVER_DDRACECOMMANDS_H
#define GAME_SERVER_DDRACECOMMANDS_H
#undef GAME_SERVER_DDRACECOMMANDS_H // this file can be included several times

#ifndef CONSOLE_COMMAND
#define CONSOLE_COMMAND(name, params, flags, callback, userdata, help, level)
#endif

CONSOLE_COMMAND("kill_pl", "v", CFGFLAG_SERVER, ConKillPlayer, this, "Kills player v and announces the kill", 2)
CONSOLE_COMMAND("logout", "v", CFGFLAG_SERVER, ConLogOut, this, "Logs player v out from the console", -1)
CONSOLE_COMMAND("helper", "v", CFGFLAG_SERVER, ConSetlvl1, this, "Authenticates player v to the level of 1", 2)
CONSOLE_COMMAND("moder", "v", CFGFLAG_SERVER, ConSetlvl2, this, "Authenticates player v to the level of 2", 3)
CONSOLE_COMMAND("admin", "v", CFGFLAG_SERVER, ConSetlvl3, this, "Authenticates player v to the level of 3 (CAUTION: Irreversible, once he is an admin you can't remove his status)", 3)
CONSOLE_COMMAND("invis", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConInvis, this, "Makes player v invisible", 2)
CONSOLE_COMMAND("vis", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConVis, this, "Makes player v visible again", 2)
CONSOLE_COMMAND("timerstop", "v", CFGFLAG_SERVER|CMDFLAG_TIMER, ConTimerStop, this, "Stops the timer of player v", 2)
CONSOLE_COMMAND("timerstart", "v", CFGFLAG_SERVER|CMDFLAG_TIMER, ConTimerStart, this, "Starts the timer of player v", 2)
CONSOLE_COMMAND("timerrestart", "v", CFGFLAG_SERVER|CMDFLAG_TIMER, ConTimerReStart, this, "Sets the timer of player v to 0 and starts it", 2)
CONSOLE_COMMAND("timerzero", "v", CFGFLAG_SERVER|CMDFLAG_TIMER, ConTimerZero, this, "Sets the timer of player v to 0 and stops it", 2)
CONSOLE_COMMAND("tele", "vi", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConTeleport, this, "Teleports player v to player i", 2)
CONSOLE_COMMAND("freeze", "v?i", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConFreeze, this, "Freezes player v for i seconds (infinite by default)", 2)
CONSOLE_COMMAND("unfreeze", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConUnFreeze, this, "Unfreezes player v", 2)
CONSOLE_COMMAND("addweapon", "v?i", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConAddWeapon, this, "Gives weapon with id i to player v (all = -1, hammer = 0, gun = 1, shotgun = 2, grenade = 3, rifle = 4, ninja = 5)", 1)
CONSOLE_COMMAND("removeweapon", "v?i", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConRemoveWeapon, this, "removes weapon with id i from player v (all = -1, hammer = 0, gun = 1, shotgun = 2, grenade = 3, rifle = 4)", 1)
CONSOLE_COMMAND("shotgun", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConShotgun, this, "Gives a shotgun to player v", 2)
CONSOLE_COMMAND("grenade", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConGrenade, this, "Gives a grenade launcher to player v", 2)
CONSOLE_COMMAND("rifle", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConRifle, this, "Gives a rifle to player v", 2)
CONSOLE_COMMAND("weapons", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConWeapons, this, "Gives all weapons to player v", 2)
CONSOLE_COMMAND("unshotgun", "v", CFGFLAG_SERVER|CMDFLAG_HELPERCMD, ConUnShotgun, this, "Takes the shotgun from player v", 2)
CONSOLE_COMMAND("ungrenade", "v", CFGFLAG_SERVER|CMDFLAG_HELPERCMD, ConUnGrenade, this, "Takes the grenade launcher from player v", 2)
CONSOLE_COMMAND("unrifle", "v", CFGFLAG_SERVER|CMDFLAG_HELPERCMD, ConUnRifle, this, "Takes the rifle from player v", 2)
CONSOLE_COMMAND("unweapons", "v", CFGFLAG_SERVER|CMDFLAG_HELPERCMD, ConUnWeapons, this, "Takes all weapons from player v", 2)
CONSOLE_COMMAND("ninja", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConNinja, this, "Makes player v a ninja", 2)
CONSOLE_COMMAND("hammer", "vi", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConHammer, this, "Sets the hammer power of player v to i", 2)
CONSOLE_COMMAND("super", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConSuper, this, "Makes player v super", 2)
CONSOLE_COMMAND("unsuper", "v", CFGFLAG_SERVER|CMDFLAG_HELPERCMD, ConUnSuper, this, "Removes super from player v", 2)
CONSOLE_COMMAND("left", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConGoLeft, this, "Makes you or player v move 1 tile left", 1)
CONSOLE_COMMAND("right", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConGoRight, this, "Makes you or player v move 1 tile right", 1)
CONSOLE_COMMAND("up", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConGoUp, this, "Makes you or player v move 1 tile up", 1)
CONSOLE_COMMAND("down", "v", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConGoDown, this, "Makes you or player v move 1 tile down", 1)
CONSOLE_COMMAND("move", "vii", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConMove, this, "Moves player i to the tile with x/y-number ii", 1)
CONSOLE_COMMAND("move_raw", "vii", CFGFLAG_SERVER|CMDFLAG_CHEAT|CMDFLAG_HELPERCMD, ConMoveRaw, this, "Moves player i to the point with x/y-coordinates ii", 1)
CONSOLE_COMMAND("credits", "", CFGFLAG_SERVER, ConCredits, this, "Shows the credits of the DDRace mod", -1)
CONSOLE_COMMAND("emote", "?si", CFGFLAG_SERVER, ConEyeEmote, this, "Sets your tee's eye emote", -1)
CONSOLE_COMMAND("broadmsg", "", CFGFLAG_SERVER, ConToggleBroadcast, this, "Toggles showing the server's broadcast message during race on/off", -1)
CONSOLE_COMMAND("eyeemote", "", CFGFLAG_SERVER, ConEyeEmote, this, "Toggles use of standard eye-emotes on/off", -1)
CONSOLE_COMMAND("settings", "?s", CFGFLAG_SERVER, ConSettings, this, "Shows gameplay information for this server", -1)
CONSOLE_COMMAND("fly", "", CFGFLAG_SERVER, ConToggleFly, this, "Toggles super-fly (holding space) on/off", 1)
CONSOLE_COMMAND("help", "?r", CFGFLAG_SERVER, ConHelp, this, "Shows help to command r, general help if left blank", -1)
CONSOLE_COMMAND("info", "", CFGFLAG_SERVER, ConInfo, this, "Shows info about this server", -1)
CONSOLE_COMMAND("kill", "", CFGFLAG_SERVER, ConKill, this, "Kills you", -1)
CONSOLE_COMMAND("me", "r", CFGFLAG_SERVER, ConMe, this, "Like the famous irc command '/me says hi' will display '<yourname> says hi'", -1)
CONSOLE_COMMAND("pause", "", CFGFLAG_SERVER, ConTogglePause, this, "Toggles pause on/off (if activated on server)", -1)
CONSOLE_COMMAND("rank", "?r", CFGFLAG_SERVER, ConRank, this, "Shows the rank of player with name r (your rank by default)", -1)
CONSOLE_COMMAND("rules", "", CFGFLAG_SERVER, ConRules, this, "Shows the server rules", -1)
CONSOLE_COMMAND("team", "?i", CFGFLAG_SERVER, ConJoinTeam, this, "Lets you join team i (shows your team if left blank)", -1)
CONSOLE_COMMAND("top5", "?i", CFGFLAG_SERVER, ConTop5, this, "Shows five ranks of the ladder beginning with rank i (1 by default)", -1)
#if defined(CONF_SQL)
CONSOLE_COMMAND("times", "?s?i", CFGFLAG_SERVER, ConTimes, this, "/times ?s?i shows last 5 times of the server or of a player beginning with name s starting with time i (i = 1 by default)", -1)
#endif
CONSOLE_COMMAND("showothers", "", CFGFLAG_SERVER, ConShowOthers, this, "Whether to showplayers from other teams or not (off by default)", -1)

CONSOLE_COMMAND("ask", "s", CFGFLAG_SERVER, ConAsk, this, "Ask to join a player in a team or to start one with him the asker is the leader if the player is not already in a team", -1)
CONSOLE_COMMAND("yes", "", CFGFLAG_SERVER, ConYes, this, "Reply yes", -1)
CONSOLE_COMMAND("no", "", CFGFLAG_SERVER, ConNo, this, "Reply no", -1)
CONSOLE_COMMAND("strict", "", CFGFLAG_SERVER, ConToggleStrict, this, "Toggle Strictness", -1)
CONSOLE_COMMAND("invite", "s", CFGFLAG_SERVER, ConInvite, this, "Invite a player to your team (You must be the leader)", -1)
CONSOLE_COMMAND("mute", "", CFGFLAG_SERVER, ConMute, this, "", 2);
CONSOLE_COMMAND("muteid", "vi", CFGFLAG_SERVER, ConMuteID, this, "", 2);
CONSOLE_COMMAND("muteip", "si", CFGFLAG_SERVER, ConMuteIP, this, "", 2);
CONSOLE_COMMAND("unmute", "i", CFGFLAG_SERVER, ConUnmute, this, "", 2);
CONSOLE_COMMAND("mutes", "", CFGFLAG_SERVER, ConMutes, this, "", 2);

#undef CONSOLE_COMMAND

#endif
