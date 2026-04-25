import discord, os, random, io, asyncio, json, time
import aiohttp
from discord.ext import commands, tasks
from discord import app_commands
from datetime import datetime, timedelta, timezone
import firebase_admin
from firebase_admin import credentials, db as fdb
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='!', intents=intents)

C = {'a': 0x57f287, 'd': 0xed4245, 'c': 0x4f545c, 'pr': 0x5865f2, 'gold': 0xf1c40f}

TEAM_ROLES: dict[str, int] = {
    'netherlands': 1497667830827585699, 'scotland': 1497667845579079720,
    'ukraine': 1497668374812037182, 'wales': 1497668509340143687,
    'turkiye': 1497668377307512932, 'switzerland': 1497668372568211588,
    'sweden': 1497668370403954829, 'spain': 1497668368193425419,
    'slovenia': 1497668365874106569, 'serbia': 1497667848389267486,
    'romania': 1497667843125415976, 'portugal': 1497667839463653707,
    'poland': 1497667836812722327, 'france': 1497667818257387560,
    'norway': 1497667833759399956, 'hungary': 1497667824460628029,
    'italy': 1497667827652497468, 'germany': 1497667821952303336,
    'england': 1497667814683840513, 'denmark': 1497667812758650940,
    'albania': 1497667804495876427, 'austria': 1497667781045260298,
    'belgium': 1497667807750393997, 'croatia': 1497667810313240828,
}
REFEREE_ROLE_ID = 1476678423479975966
MANAGER_ROLE_ID = 1496519260296450200
ASST_ROLE_ID = 1476677267856818236
LINKS_DATA = [
    ("🎮 Game",           "Play RFA on Roblox",           "https://www.roblox.com/games/126519602812261/World#!/about",   None),
    ("👥 Group",          "Join the Roblox group",        "https://www.roblox.com/communities/35056215/Realistic-Football-Association#!/about", None),
    ("🏠 Main Server",    "Join the main Discord",        "https://discord.gg/ABUpNSYqqa",                               None),
    ("⚖️ Appeal Server",  "Appeal a ban",                 "https://discord.gg/NSst6PsNkB",                               None),
    ("▶️ YouTube",        "Watch RFA videos",             "https://www.youtube.com/@RoFootballAssociation",              None),
    ("📋 Team Template",  "Set up a team server",         "https://discord.new/edkZJ3xsKshA",                            [MANAGER_ROLE_ID, ASST_ROLE_ID]),
    ("🏁 Referee Server", "Join the referee server",      "https://discord.gg/p2eygmHR3g",                               [REFEREE_ROLE_ID]),
]


TEAM_FLAGS: dict[str, str] = {
    'netherlands': '🇳🇱', 'scotland': '🏴󠁧󠁢󠁳󠁣󠁴󠁿', 'ukraine': '🇺🇦',
    'wales': '🏴󠁧󠁢󠁷󠁬󠁳󠁿', 'turkiye': '🇹🇷', 'switzerland': '🇨🇭',
    'sweden': '🇸🇪', 'spain': '🇪🇸', 'slovenia': '🇸🇮',
    'serbia': '🇷🇸', 'romania': '🇷🇴', 'portugal': '🇵🇹',
    'poland': '🇵🇱', 'france': '🇫🇷', 'norway': '🇳🇴',
    'hungary': '🇭🇺', 'italy': '🇮🇹', 'germany': '🇩🇪',
    'england': '🏴󠁧󠁢󠁥󠁮󠁧󠁿', 'denmark': '🇩🇰', 'albania': '🇦🇱',
    'austria': '🇦🇹', 'belgium': '🇧🇪', 'croatia': '🇭🇷',
}

TEAM_CHOICES = [app_commands.Choice(name=k.title(), value=k) for k in sorted(TEAM_ROLES.keys())]

STAFF_ROLE_ID = 1475565079767290040
FREE_AGENT_CHANNEL_ID = 1292595174232424518
FREE_AGENT_COOLDOWN = 5 * 60 * 60
CONTRACT_LOG_CHANNEL_ID = 1476037356917227782
FRIENDLIES_CHANNEL_ID = 1477028031317934190
FRIENDLY_PING_ROLE_ID = 1293674314876325930
SCOUT_CHANNEL_ID = 1292595273884897377

def _require(key):
    val = os.environ.get(key)
    if not val: raise RuntimeError(f'Missing env var: {key}')
    return val

BOT_TOKEN = _require('BOT_TOKEN')
ROBLOX_UNIVERSE = _require('ROBLOX_UNIVERSE')
ROBLOX_GROUP_ID = _require('ROBLOX_GROUP_ID')
ROBLOX_API_KEY = _require('ROBLOX_API_KEY')
DISCORD_GUILD_ID = int(_require('DISCORD_GUILD_ID'))
ROVER_SECRET = _require('ROVER_SECRET')

def _init_firebase():
    raw = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not raw: raise RuntimeError('Missing GOOGLE_APPLICATION_CREDENTIALS')
    try: cred_dict = json.loads(raw)
    except json.JSONDecodeError as e: raise RuntimeError(f'Invalid JSON: {e}')
    if 'private_key' in cred_dict:
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
    firebase_admin.initialize_app(credentials.Certificate(cred_dict), {
        'databaseURL': 'https://rfa-base-default-rtdb.europe-west1.firebasedatabase.app/'
    })

_init_firebase()

def _r(path): return fdb.reference(path)
def _now(): return datetime.now(timezone.utc).isoformat()

def get_fa_cooldown(guild_id, user_id):
    raw = _r(f'rfa/{guild_id}/fa_cooldowns/{user_id}').get()
    return float(raw) if raw else 0.0

def set_fa_cooldown(guild_id, user_id):
    _r(f'rfa/{guild_id}/fa_cooldowns/{user_id}').set(time.time())

def audit_log(guild_id, action, data):
    try: _r(f'rfa/{guild_id}/audit_log').push({'action': action, 'timestamp': _now(), **data})
    except Exception as e: print(f'[audit_log] {e}')

def tfmt(team):
    flag = TEAM_FLAGS.get(team.lower(), '')
    return f"{flag} {team.title()}".strip()

def get_team_role(guild, team):
    rid = TEAM_ROLES.get(team.lower())
    return guild.get_role(rid) if rid else None

def get_member_team(member):
    ids = {r.id for r in member.roles}
    for team, rid in TEAM_ROLES.items():
        if rid in ids: return team
    return None

def is_manager(member):
    ids = {r.id for r in member.roles}
    return MANAGER_ROLE_ID in ids or ASST_ROLE_ID in ids

def is_staff(member):
    ids = {r.id for r in member.roles}
    return STAFF_ROLE_ID in ids or member.guild_permissions.administrator

def is_manager_of(member, team):
    if member.guild_permissions.administrator: return True
    ids = {r.id for r in member.roles}
    return (MANAGER_ROLE_ID in ids or ASST_ROLE_ID in ids) and TEAM_ROLES.get(team.lower()) in ids

def get_manager_team(member):
    if not is_manager(member): return None
    for team, rid in TEAM_ROLES.items():
        if rid in {r.id for r in member.roles}: return team
    return None

def get_team_roster(guild, team):
    role = get_team_role(guild, team)
    return [m for m in role.members if not m.bot] if role else []

def signing_open(guild_id):
    raw = _r(f'rfa/{guild_id}/cfg/open').get()
    return bool(raw) if raw is not None else True

def set_signing(guild_id, val):
    _r(f'rfa/{guild_id}/cfg/open').set(int(val))

def get_max_players(guild_id):
    raw = _r(f'rfa/{guild_id}/cfg/maxp').get()
    return int(raw) if raw else 25

def set_max_players(guild_id, val):
    _r(f'rfa/{guild_id}/cfg/maxp').set(val)

def footer(guild):
    icon = guild.icon.url if guild and guild.icon else None
    return 'Roblox Football Association', icon

def roblox_headers():
    return {'x-api-key': ROBLOX_API_KEY, 'Content-Type': 'application/json'}

ROBLOX_DATASTORE = 'ModSystem'
KOHL_DS = 'KSave'
KOHL_TITLES = {1:'VIP',2:'Moderator',3:'Administrator',4:'Super Admin',5:'Owner',6:'Game Creator'}

async def roblox_get_user_id(username):
    async with aiohttp.ClientSession() as s:
        async with s.post('https://users.roblox.com/v1/usernames/users',
                          json={'usernames': [username], 'excludeBannedUsers': False}) as r:
            if r.status != 200: return None
            users = (await r.json()).get('data', [])
            return users[0]['id'] if users else None

async def roblox_get_user_info(user_id):
    async with aiohttp.ClientSession() as s:
        async with s.get(f'https://users.roblox.com/v1/users/{user_id}') as r:
            return await r.json() if r.status == 200 else None

async def roblox_ban(user_id, reason, duration_days):
    payload = {'gameJoinRestriction': {'active': True, 'privateReason': reason, 'displayReason': reason}}
    if duration_days: payload['gameJoinRestriction']['duration'] = f'{duration_days * 86400}s'
    async with aiohttp.ClientSession() as s:
        async with s.patch(
            f'https://apis.roblox.com/cloud/v2/universes/{ROBLOX_UNIVERSE}/user-restrictions/{user_id}',
            headers=roblox_headers(), json=payload) as r:
            return (True, 'OK') if r.status in (200, 204) else (False, f'HTTP {r.status}: {await r.text()}')

async def roblox_unban(user_id):
    async with aiohttp.ClientSession() as s:
        async with s.patch(
            f'https://apis.roblox.com/cloud/v2/universes/{ROBLOX_UNIVERSE}/user-restrictions/{user_id}',
            headers=roblox_headers(), json={'gameJoinRestriction': {'active': False}}) as r:
            return (True, 'OK') if r.status in (200, 204) else (False, f'HTTP {r.status}: {await r.text()}')

async def roblox_get_ban(user_id):
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://apis.roblox.com/cloud/v2/universes/{ROBLOX_UNIVERSE}/user-restrictions/{user_id}',
            headers={'x-api-key': ROBLOX_API_KEY}) as r:
            return await r.json() if r.status == 200 else None

async def roblox_get_all_bans():
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://apis.roblox.com/cloud/v2/universes/{ROBLOX_UNIVERSE}/user-restrictions',
            headers={'x-api-key': ROBLOX_API_KEY}, params={'maxPageSize': 100}) as r:
            if r.status != 200: return []
            return [e for e in (await r.json()).get('userRestrictions', [])
                    if e.get('gameJoinRestriction', {}).get('active')]

async def roblox_get_player_count():
    async with aiohttp.ClientSession() as s:
        async with s.get(f'https://games.roblox.com/v1/games?universeIds={ROBLOX_UNIVERSE}') as r:
            if r.status != 200: return None
            games = (await r.json()).get('data', [])
            return games[0].get('playing') if games else None

async def roblox_get_servers():
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://games.roblox.com/v1/games/{ROBLOX_UNIVERSE}/servers/Public?limit=10') as r:
            return (await r.json()).get('data', []) if r.status == 200 else []

async def roblox_announce(topic, message):
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f'https://apis.roblox.com/messaging-service/v1/universes/{ROBLOX_UNIVERSE}/topics/{topic}',
            headers=roblox_headers(), json={'message': message}) as r:
            return (True, 'OK') if r.status in (200, 204) else (False, f'HTTP {r.status}: {await r.text()}')

async def roblox_message(topic, payload):
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f'https://apis.roblox.com/messaging-service/v1/universes/{ROBLOX_UNIVERSE}/topics/{topic}',
            headers=roblox_headers(), json={'message': json.dumps(payload)}) as r:
            return (True, 'OK') if r.status in (200, 204) else (False, f'HTTP {r.status}: {await r.text()}')

async def ds_set(key, value, store=None):
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f'https://apis.roblox.com/datastores/v1/universes/{ROBLOX_UNIVERSE}/standard-datastores/datastore/entries/entry',
            headers=roblox_headers(),
            params={'datastoreName': store or ROBLOX_DATASTORE, 'entryKey': key},
            json=value) as r:
            return (True, 'OK') if r.status in (200, 201) else (False, f'HTTP {r.status}: {await r.text()}')

async def ds_delete(key, store=None):
    async with aiohttp.ClientSession() as s:
        async with s.delete(
            f'https://apis.roblox.com/datastores/v1/universes/{ROBLOX_UNIVERSE}/standard-datastores/datastore/entries/entry',
            headers={'x-api-key': ROBLOX_API_KEY},
            params={'datastoreName': store or ROBLOX_DATASTORE, 'entryKey': key}) as r:
            return (True, 'OK') if r.status in (200, 204) else (False, f'HTTP {r.status}: {await r.text()}')

async def ds_list(prefix='', store=None):
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://apis.roblox.com/datastores/v1/universes/{ROBLOX_UNIVERSE}/standard-datastores/datastore/entries',
            headers={'x-api-key': ROBLOX_API_KEY},
            params={'datastoreName': store or ROBLOX_DATASTORE, 'prefix': prefix, 'limit': 100}) as r:
            return (await r.json()).get('keys', []) if r.status == 200 else []

async def ds_get(key, store=None):
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://apis.roblox.com/datastores/v1/universes/{ROBLOX_UNIVERSE}/standard-datastores/datastore/entries/entry',
            headers={'x-api-key': ROBLOX_API_KEY},
            params={'datastoreName': store or ROBLOX_DATASTORE, 'entryKey': key}) as r:
            return await r.json() if r.status == 200 else None

async def kohl_read():
    raw = await ds_get('KSave', store=KOHL_DS)
    if not raw or not isinstance(raw, list) or len(raw) < 1: return {}
    admins = {}
    for part in (raw[0] or '').strip().split():
        try:
            uid, power = part.split(':')
            admins[int(uid)] = int(power)
        except: continue
    return admins

async def kohl_write(admins):
    raw = await ds_get('KSave', store=KOHL_DS)
    if not raw or not isinstance(raw, list): raw = ['', '', '']
    raw[0] = ''.join(f' {uid}:{power}' for uid, power in admins.items() if power != 0)
    return await ds_set('KSave', raw, store=KOHL_DS)

async def kohl_set_power(user_id, power):
    admins = await kohl_read()
    if power == 0: admins.pop(user_id, None)
    else: admins[user_id] = power
    return await kohl_write(admins)

async def kohl_get_username(user_id):
    info = await roblox_get_user_info(user_id)
    return info.get('name', str(user_id)) if info else str(user_id)

async def upload_image_to_roblox(image_bytes, filename, name):
    metadata = {
        "assetType": "Decal", "displayName": name,
        "description": "Uploaded via RFA Discord bot",
        "creationContext": {"creator": {"groupId": ROBLOX_GROUP_ID}}
    }
    form = aiohttp.FormData()
    form.add_field('request', json.dumps(metadata), content_type='application/json')
    form.add_field('fileContent', image_bytes, filename=filename, content_type='image/png')
    async with aiohttp.ClientSession() as s:
        async with s.post('https://apis.roblox.com/assets/v1/assets',
                          headers={'x-api-key': ROBLOX_API_KEY}, data=form) as r:
            if r.status not in (200, 201):
                return False, f'Upload failed: HTTP {r.status} — {await r.text()}'
            return True, (await r.json()).get('path', '')

async def poll_asset_operation(operation_path):
    async with aiohttp.ClientSession() as s:
        for _ in range(15):
            await asyncio.sleep(3)
            async with s.get(f'https://apis.roblox.com/{operation_path}',
                             headers={'x-api-key': ROBLOX_API_KEY}) as r:
                if r.status != 200: continue
                data = await r.json()
                if data.get('done'):
                    asset_id = data.get('response', {}).get('assetId')
                    return (True, str(asset_id)) if asset_id else (False, f'No assetId: {data}')
    return False, 'Timed out waiting for Roblox to process the image'

async def grant_pin_to_player(roblox_user_id, asset_id):
    ds_key = f'pins_{roblox_user_id}'
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://apis.roblox.com/datastores/v1/universes/{ROBLOX_UNIVERSE}/standard-datastores/datastore/entries/entry',
            headers={'x-api-key': ROBLOX_API_KEY},
            params={'datastoreName': 'PlayerPins_v1', 'entryKey': ds_key}) as r:
            current = (await r.json()) if r.status == 200 else []
            if not isinstance(current, list): current = []
    asset_id_int = int(asset_id)
    if asset_id_int not in current: current.append(asset_id_int)
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f'https://apis.roblox.com/datastores/v1/universes/{ROBLOX_UNIVERSE}/standard-datastores/datastore/entries/entry',
            headers={'x-api-key': ROBLOX_API_KEY, 'Content-Type': 'application/json'},
            params={'datastoreName': 'PlayerPins_v1', 'entryKey': ds_key},
            json=current) as r:
            if r.status not in (200, 201):
                return False, f'DataStore write failed: HTTP {r.status} — {await r.text()}'
    await roblox_message('PinGranted', {'userId': roblox_user_id, 'assetId': asset_id_int})
    return True, 'OK'

class SignView(discord.ui.View):
    def __init__(self, cid, guild_id, player_id):
        super().__init__(timeout=None)
        self.cid = cid
        self.guild_id = guild_id
        self.player_id = player_id
        self.accept_btn.custom_id = f'sign_a_{guild_id}_{cid}'
        self.decline_btn.custom_id = f'sign_d_{guild_id}_{cid}'

    async def interaction_check(self, it):
        if it.user.id != self.player_id:
            await it.response.send_message('This contract is not addressed to you.', ephemeral=True)
            return False
        return True

    async def _resolve(self, it, accepted):
        row = _r(f'rfa/{self.guild_id}/contracts/{self.cid}').get()
        if not row or row.get('status') != 'Pending':
            await it.response.send_message('This contract is no longer active.', ephemeral=True)
            return
        status = 'Signed' if accepted else 'Rejected'
        _r(f'rfa/{self.guild_id}/contracts/{self.cid}').update({'status': status, 'responded': _now()})
        guild = bot.get_guild(self.guild_id)
        col = C['a'] if accepted else C['d']
        team = row['team']
        if accepted and guild:
            team_role = get_team_role(guild, team)
            if team_role:
                try:
                    member = guild.get_member(row['sg_id'])
                    if member: await member.add_roles(team_role, reason=f'Signed to {team}')
                except Exception as e: print(f'[sign] role grant failed: {e}')
        updated = _build_contract_embed(self.cid, row, col, guild, status)
        for btn in self.children: btn.disabled = True
        await it.response.edit_message(embed=updated, view=self)
        verb = 'accepted' if accepted else 'declined'
        verb_past = 'Accepted' if accepted else 'Declined'
        player_dm_embed = discord.Embed(
            color=col, title=f'Contract {verb_past}',
            description=(f'You have **{verb}** the contract offer to join **{tfmt(team)}**.\n\n'
                         f'Position: {row.get("pos","—")}\nRole: {row.get("tier","—")}\nContract ID: `{self.cid}`')
        )
        ft, fi = footer(guild)
        player_dm_embed.set_footer(text=ft, icon_url=fi)
        if row.get('dm_msg_id'):
            try:
                u = await bot.fetch_user(row['sg_id'])
                dm = await u.create_dm()
                dm_msg = await dm.fetch_message(row['dm_msg_id'])
                await dm_msg.edit(embed=player_dm_embed, view=discord.ui.View())
            except: pass
        manager_dm_embed = discord.Embed(
            color=col, title=f'Contract {verb_past}',
            description=(f'<@{row["sg_id"]}> has **{verb}** your contract offer for **{tfmt(team)}**.\n\n'
                         f'Position: {row.get("pos","—")}\nRole: {row.get("tier","—")}\nContract ID: `{self.cid}`')
        )
        manager_dm_embed.set_footer(text=ft, icon_url=fi)
        try:
            contractor = await bot.fetch_user(row['ct_id'])
            await contractor.send(embed=manager_dm_embed)
        except: pass
        try:
            log_ch = bot.get_channel(CONTRACT_LOG_CHANNEL_ID)
            if log_ch:
                pn = row.get('sg_name', str(row['sg_id']))
                mn = row.get('ct_name', str(row['ct_id']))
                if accepted: await log_ch.send(f'__**{pn}**__ has accepted the offer from **{mn}** to join **{tfmt(team)}**.')
                else: await log_ch.send(f'__**{pn}**__ has declined the offer from **{mn}** to join **{tfmt(team)}**.')
        except Exception as e: print(f'[contract log] {e}')
        try:
            orig_ch = bot.get_channel(row.get('ch_id'))
            if orig_ch and row.get('msg_id'):
                orig_msg = await orig_ch.fetch_message(row['msg_id'])
                await orig_msg.edit(
                    content=f'Contract {verb_past} — <@{row["sg_id"]}> has **{verb}** the offer from <@{row["ct_id"]}> to join **{tfmt(team)}**.',
                    embed=updated, view=self)
        except: pass

    @discord.ui.button(label='Accept', style=discord.ButtonStyle.success, custom_id='sign_a_placeholder')
    async def accept_btn(self, it, _): await self._resolve(it, True)

    @discord.ui.button(label='Decline', style=discord.ButtonStyle.danger, custom_id='sign_d_placeholder')
    async def decline_btn(self, it, _): await self._resolve(it, False)

def _build_contract_embed(cid, row, color, guild, status=None):
    status_line = {'Signed':'\n\nContract Accepted','Rejected':'\n\nContract Declined',
                   'Expired':'\n\nContract Expired','Cancelled':'\n\nContract Revoked'}.get(status,'')
    desc = ('This document serves as an official binding agreement between the Player and the RFA Manager. '
            'Upon acceptance, the player commits to representing their assigned nation with full dedication.' + status_line)
    e = discord.Embed(title='Contract Offer — RFA', color=color, description=desc)
    e.add_field(name='Player', value=f'<@{row["sg_id"]}>', inline=True)
    e.add_field(name='Team', value=tfmt(row['team']), inline=True)
    e.add_field(name='Position', value=row.get('pos','—'), inline=True)
    e.add_field(name='Role', value=row.get('tier','—'), inline=True)
    e.add_field(name='Contract ID', value=f'`{cid}`', inline=True)
    if row.get('notes'): e.add_field(name='Notes', value=row['notes'], inline=False)
    team_role = get_team_role(guild, row['team']) if guild else None
    if team_role: e.add_field(name='Discord Role', value=team_role.mention, inline=False)
    ft, fi = footer(guild)
    e.set_footer(text=ft, icon_url=fi)
    return e

@tasks.loop(seconds=30)
async def expire_loop():
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    all_rfa = _r('rfa').get() or {}
    for gid_str, gdata in all_rfa.items():
        gid = int(gid_str)
        for cid, row in (gdata.get('contracts') or {}).items():
            if row.get('status') != 'Pending': continue
            if row.get('created','') >= cutoff: continue
            _r(f'rfa/{gid}/contracts/{cid}').update({'status':'Expired','responded':_now()})
            guild = bot.get_guild(gid)
            e = _build_contract_embed(cid, row, C['c'], guild, 'Expired')
            blank = discord.ui.View()
            team = row.get('team','')
            if guild and row.get('ch_id') and row.get('msg_id'):
                ch = guild.get_channel(row['ch_id'])
                if ch:
                    try:
                        msg = await ch.fetch_message(row['msg_id'])
                        await msg.edit(
                            content=f'Contract Expired — the offer to <@{row["sg_id"]}> from <@{row["ct_id"]}> to join **{tfmt(team)}** has expired.',
                            embed=e, view=blank)
                    except: pass
            if row.get('dm_msg_id'):
                try:
                    u = await bot.fetch_user(row['sg_id'])
                    dm = await u.create_dm()
                    dm_msg = await dm.fetch_message(row['dm_msg_id'])
                    exp_embed = discord.Embed(color=C['c'], title='Contract Expired',
                        description=(f'Your contract offer to join **{tfmt(team)}** has expired.\n\n'
                                     f'Position: {row.get("pos","—")}\nRole: {row.get("tier","—")}\nContract ID: `{cid}`'))
                    ft, fi = footer(guild)
                    exp_embed.set_footer(text=ft, icon_url=fi)
                    await dm_msg.edit(embed=exp_embed, view=blank)
                except: pass
            try:
                contractor = await bot.fetch_user(row['ct_id'])
                mgr_embed = discord.Embed(color=C['c'], title='Contract Expired',
                    description=(f'The contract offer to <@{row["sg_id"]}> for **{tfmt(team)}** has expired.\n\n'
                                 f'Position: {row.get("pos","—")}\nRole: {row.get("tier","—")}\nContract ID: `{cid}`'))
                ft, fi = footer(guild)
                mgr_embed.set_footer(text=ft, icon_url=fi)
                await contractor.send(embed=mgr_embed)
            except: pass
            try:
                log_ch = bot.get_channel(CONTRACT_LOG_CHANNEL_ID)
                if log_ch:
                    pn = row.get('sg_name', str(row['sg_id']))
                    mn = row.get('ct_name', str(row['ct_id']))
                    await log_ch.send(f'__**{pn}**__ did not respond to the offer from **{mn}** to join **{tfmt(team)}**. The contract has expired.')
            except Exception as ex: print(f'[contract log expire] {ex}')

class CloseReasonModal(discord.ui.Modal, title='Close Ticket'):
    reason = discord.ui.TextInput(
        label='Reason for closing',
        style=discord.TextStyle.paragraph,
        placeholder='Explain why this ticket is being closed…',
        required=True,
        max_length=500,
    )

    def __init__(self, channel_id: int, guild_id: int):
        super().__init__()
        self.channel_id = channel_id
        self.guild_id = guild_id

    async def on_submit(self, it: discord.Interaction):
        close_reason = self.reason.value
        await it.response.defer()
        tk = _r(f'rfa/{self.guild_id}/tickets/{self.channel_id}').get()
        if not tk:
            await it.followup.send('Not a ticket channel.', ephemeral=True)
            return
        channel = it.guild.get_channel(self.channel_id)
        if not channel:
            await it.followup.send('Channel not found.', ephemeral=True)
            return
        lines = []
        async for m in channel.history(limit=500, oldest_first=True):
            lines.append(f'[{m.created_at.strftime("%Y-%m-%d %H:%M:%S")}] {m.author.display_name}: {m.content or "[embed/attachment]"}')
        transcript_bytes = '\n'.join(lines).encode()
        transcript_file_staff = discord.File(io.BytesIO(transcript_bytes), filename=f'transcript-{channel.name}.txt')
        transcript_file_user = discord.File(io.BytesIO(transcript_bytes), filename=f'transcript-{channel.name}.txt')
        ft, fi = footer(it.guild)
        log_ch_id = _r(f'rfa/{self.guild_id}/cfg/tlog').get()
        if log_ch_id:
            lch = it.guild.get_channel(int(log_ch_id))
            if lch:
                le = discord.Embed(color=C['c'], description=f'Ticket closed by {it.user.mention}')
                le.add_field(name='Opened by', value=f'<@{tk["uid"]}>', inline=True)
                le.add_field(name='Channel', value=channel.name, inline=True)
                le.add_field(name='Opened', value=tk['created'][:10], inline=True)
                le.add_field(name='Close Reason', value=close_reason, inline=False)
                le.set_footer(text=ft, icon_url=fi)
                await lch.send(embed=le, file=transcript_file_staff)
        try:
            creator = await bot.fetch_user(tk['uid'])
            dm_embed = discord.Embed(
                color=C['c'],
                title='Your Ticket Has Been Closed',
                description=(
                    f'Your ticket in **{it.guild.name}** has been closed by {it.user.mention}.\n\n'
                    f'**Reason:**\n{close_reason}\n\n'
                    f'A transcript of the conversation is attached below.'
                )
            )
            dm_embed.set_footer(text=ft, icon_url=fi)
            await creator.send(embed=dm_embed, file=transcript_file_user)
        except Exception as e:
            print(f'[ticket close dm] {e}')
        _r(f'rfa/{self.guild_id}/tickets/{self.channel_id}').update({
            'status': 'closed',
            'closed': _now(),
            'close_reason': close_reason,
            'closed_by': it.user.id,
        })
        await channel.send('Closing in 3 seconds…')
        await asyncio.sleep(3)
        await channel.delete()

class CloseTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Close Ticket', style=discord.ButtonStyle.danger, emoji='🔒', custom_id='close_ticket_btn')
    async def close(self, it: discord.Interaction, _):
        tk = _r(f'rfa/{it.guild_id}/tickets/{it.channel_id}').get()
        if not tk:
            await it.response.send_message('Not a ticket channel.', ephemeral=True)
            return
        can_close = (
            it.user.guild_permissions.manage_channels
            or it.user.guild_permissions.administrator
            or is_staff(it.user)
            or it.user.id == tk['uid']
        )
        if not can_close:
            await it.response.send_message('You do not have permission to close this ticket.', ephemeral=True)
            return
        modal = CloseReasonModal(channel_id=it.channel_id, guild_id=it.guild_id)
        await it.response.send_modal(modal)

class TicketReasonSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label='Team Management', value='Team Management', emoji='⚽', description='Issues related to team rosters, signings, or releases'),
            discord.SelectOption(label='Support',         value='Support',         emoji='🛠️', description='General help or bot-related support'),
            discord.SelectOption(label='Report',          value='Report',          emoji='🚨', description='Report a player, manager, or issue'),
            discord.SelectOption(label='Other',           value='Other',           emoji='📋', description='Anything else not listed above'),
        ]
        super().__init__(
            placeholder='Select a reason for opening this ticket…',
            min_values=1,
            max_values=1,
            options=options,
            custom_id='ticket_reason_select',
        )

    async def callback(self, it: discord.Interaction):
        await it.response.defer(ephemeral=True)

        reason = self.values[0]
        guild_id = it.guild_id
        tcat = _r(f'rfa/{guild_id}/cfg/tcat').get()
        if not tcat:
            await it.followup.send('Ticket system not configured.', ephemeral=True)
            return

        # Check for existing open ticket
        tickets = _r(f'rfa/{guild_id}/tickets').get() or {}
        for ch_id, tk in tickets.items():
            if tk.get('uid') == it.user.id and tk.get('status') == 'open':
                ch = it.guild.get_channel(int(ch_id))
                if ch:
                    await it.followup.send(f'You already have an open ticket: {ch.mention}', ephemeral=True)
                    return

        base_category = it.guild.get_channel(int(tcat))
        if not base_category or not isinstance(base_category, discord.CategoryChannel):
            await it.followup.send('Ticket category not found.', ephemeral=True)
            return

        # Find or create a category with free slots
        async def get_available_category(guild, category):
            if len(category.channels) < 50:
                return category
            overflow_name = f'{category.name} Overflow'
            overflow = discord.utils.get(guild.categories, name=overflow_name)
            if overflow and len(overflow.channels) < 50:
                return overflow
            overflow = await guild.create_category(overflow_name)
            # Copy permissions from the original category
            await overflow.edit(overwrites=category.overwrites)
            return overflow

        target_category = await get_available_category(it.guild, base_category)

        staff_role = it.guild.get_role(STAFF_ROLE_ID)
        ow = {
            it.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            it.user: discord.PermissionOverwrite(view_channel=True, send_messages=True, attach_files=True),
            it.guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True),
        }
        if staff_role:
            ow[staff_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True)

        # Create the ticket channel
        try:
            ch = await it.guild.create_text_channel(
                f'ticket-{it.user.name}', category=target_category, overwrites=ow
            )
        except discord.Forbidden:
            await it.followup.send('I lack permission to create channels.', ephemeral=True)
            return
        except Exception as e:
            await it.followup.send(f'Failed to create channel: {e}', ephemeral=True)
            return

        _r(f'rfa/{guild_id}/tickets/{ch.id}').set({
            'uid': it.user.id,
            'status': 'open',
            'created': _now(),
            'closed': None,
            'reason': reason,
        })

        ft, fi = footer(it.guild)
        e = discord.Embed(
            color=C['pr'],
            title=f'Ticket — {reason}',
            description=(
                f'Welcome {it.user.mention}! A member of staff will be with you shortly.\n\n'
                f'**Reason:** {reason}\n\n'
                f'Please describe your issue in as much detail as possible.'
            )
        )
        e.set_footer(text=ft, icon_url=fi)
        await ch.send(embed=e, view=CloseTicketView())
        await it.followup.send(f'Your ticket has been created: {ch.mention}', ephemeral=True)

class TicketPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Open Ticket', style=discord.ButtonStyle.primary, emoji='🎫', custom_id='open_ticket_btn')
    async def open_ticket(self, it: discord.Interaction, _):
        tcat = _r(f'rfa/{it.guild_id}/cfg/tcat').get()
        if not tcat:
            await it.response.send_message('Ticket system not configured.', ephemeral=True)
            return
        view = discord.ui.View(timeout=120)
        view.add_item(TicketReasonSelect())
        await it.response.send_message(
            '**Select a reason for your ticket:**',
            view=view,
            ephemeral=True,
        )

@bot.tree.command(name='contract', description='Send a contract offer to a player', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.describe(player='The player to offer a contract to', pos='Position (e.g. GK, CB, ST)', tier='Role (e.g. Starter, Sub, Backup)', notes='Optional notes')
@app_commands.default_permissions(manage_messages=True)
async def sign_cmd(it, player: discord.Member, pos: str, tier: str, notes: str = None):
    if not is_manager(it.user) and not it.user.guild_permissions.administrator:
        await it.response.send_message('You must have the **Manager** or **Assistant Manager** role to sign players.', ephemeral=True); return
    team = get_manager_team(it.user)
    if team is None and not it.user.guild_permissions.administrator:
        await it.response.send_message('You must also have your **team role** assigned. Contact an administrator.', ephemeral=True); return
    if team is None:
        await it.response.send_message('Administrators without a team role should use `/forceadd` instead.', ephemeral=True); return
    if player.id == it.user.id:
        await it.response.send_message('You cannot sign yourself.', ephemeral=True); return
    if player.bot:
        await it.response.send_message('Bots cannot be signed.', ephemeral=True); return
    if not signing_open(it.guild_id):
        await it.response.send_message('The signing window is currently **closed**.', ephemeral=True); return
    existing = get_member_team(player)
    if existing:
        await it.response.send_message(f'{player.mention} is already signed to **{tfmt(existing)}**.', ephemeral=True); return
    roster = get_team_roster(it.guild, team)
    if len(roster) >= get_max_players(it.guild_id):
        await it.response.send_message(f'**{tfmt(team)}** squad is full ({get_max_players(it.guild_id)} max).', ephemeral=True); return
    contracts = _r(f'rfa/{it.guild_id}/contracts').get() or {}
    for cdata in contracts.values():
        if cdata.get('sg_id') == player.id and cdata.get('status') == 'Pending':
            await it.response.send_message(f'{player.mention} already has a **pending** contract offer.', ephemeral=True); return
    cid = str(random.randint(10**15, 10**16 - 1))
    row = {
        'ct_id':it.user.id,'ct_name':it.user.name,'sg_id':player.id,'sg_name':player.name,
        'team':team,'pos':pos,'tier':tier,'notes':notes,'status':'Pending',
        'created':_now(),'responded':None,'msg_id':None,'ch_id':it.channel_id,'dm_msg_id':None,
    }
    e = _build_contract_embed(cid, row, 0x2b2d31, it.guild)
    v = SignView(cid, it.guild_id, player.id)
    await it.response.send_message(content=f'Contract offer sent to {player.mention} by {it.user.mention} | Expires in 5 minutes', embed=e, view=v)
    pub_msg = await it.original_response()
    row['msg_id'] = pub_msg.id
    dm_msg_id = None
    try:
        dm_msg = await player.send(content=f'You have a contract offer from **{it.guild.name}** to join **{tfmt(team)}**.\nHead to the server to accept or decline.', embed=e, view=SignView(cid, it.guild_id, player.id))
        dm_msg_id = dm_msg.id
    except: pass
    row['dm_msg_id'] = dm_msg_id
    _r(f'rfa/{it.guild_id}/contracts/{cid}').set(row)
    bot.add_view(v)

@bot.tree.command(name='ban', description='Ban a member from Discord and the Roblox game', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.describe(
    member='The Discord member to ban',
    roblox_username='Their Roblox username',
    reason='Reason for the ban',
    duration_days='Roblox game ban duration in days (leave empty for permanent)',
)
@app_commands.default_permissions(administrator=True)
async def ban_cmd(it: discord.Interaction, member: discord.Member, roblox_username: str, reason: str, duration_days: int = None):
    await it.response.defer()

    if member.id == it.user.id:
        await it.followup.send('You cannot ban yourself.'); return
    if member.guild_permissions.administrator:
        await it.followup.send('You cannot ban an administrator.'); return
    if member.top_role >= it.user.top_role and not it.user.guild_permissions.administrator:
        await it.followup.send('You cannot ban a member with an equal or higher role.'); return

    # -- Resolve Roblox user --
    roblox_user_id = await roblox_get_user_id(roblox_username)
    if not roblox_user_id:
        await it.followup.send(f'Roblox user **{roblox_username}** not found. Aborting.'); return

    dur_label = f'{duration_days} day(s)' if duration_days else 'Permanent'

    # -- DM the banned member BEFORE kicking them --
    dm_embed = discord.Embed(
        color=0xed4245,
        title='You have been banned',
        description=(
            f'You have been banned from **{it.guild.name}** and the Roblox game.\n\n'
            f'**Reason:** {reason}\n'
            f'**Duration:** {dur_label}\n\n'
            'If you believe this ban was issued in error, you may submit an appeal below.\n'
            'https://discord.gg/5NBkSskAwt'
        )
    )
    dm_embed.set_footer(text='Roblox Football Association')
    try:
        await member.send(embed=dm_embed)
    except (discord.Forbidden, discord.HTTPException):
        pass  # DMs closed — continue regardless

    # -- Discord ban --
    discord_banned = False
    discord_error = None
    try:
        await it.guild.ban(member, reason=f'{reason} | Banned by {it.user}', delete_message_days=0)
        discord_banned = True
    except discord.Forbidden:
        discord_error = 'Missing permissions to ban this member.'
    except discord.HTTPException as ex:
        discord_error = str(ex)

    # -- Roblox game ban --
    roblox_banned = False
    roblox_error = None
    success, msg = await roblox_ban(roblox_user_id, reason, duration_days)
    if success:
        roblox_banned = True
        _r(f'rfa/{it.guild_id}/roblox_bans/{roblox_user_id}').set({
            'username': roblox_username,
            'reason': reason,
            'duration_days': duration_days,
            'permanent': duration_days is None,
            'banned_by': it.user.id,
            'banned_at': _now(),
        })
        await roblox_message('ChatLog', {
            'scope': 'all',
            'color': 'red',
            'text': f'[BAN] {roblox_username} has been banned. Reason: {reason} ({dur_label})',
            'sender': 'RFA System',
        })
    else:
        roblox_error = msg

    # -- Audit log --
    audit_log(it.guild_id, 'ban', {
        'discord_id': member.id,
        'username': roblox_username,
        'user_id': roblox_user_id,
        'reason': reason,
        'duration': dur_label,
        'discord_banned': discord_banned,
        'roblox_banned': roblox_banned,
        'by': it.user.name,
        'by_id': it.user.id,
    })

    # -- Response embed --
    all_success = discord_banned and roblox_banned
    color = C['d'] if all_success else C['gold']

    e = discord.Embed(
        color=color,
        title='Ban Issued' if all_success else 'Ban Partially Issued',
    )
    e.add_field(name='Member', value=f'{member} ({member.id})', inline=True)
    e.add_field(name='Roblox', value=roblox_username, inline=True)
    e.add_field(name='Duration', value=dur_label, inline=True)
    e.add_field(name='Reason', value=reason, inline=False)
    e.add_field(
        name='Discord Ban',
        value='Issued' if discord_banned else f'Failed — {discord_error}',
        inline=True,
    )
    e.add_field(
        name='Roblox Ban',
        value='Issued' if roblox_banned else f'Failed — {roblox_error}',
        inline=True,
    )
    e.add_field(name='Banned by', value=it.user.mention, inline=False)

    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)

    await it.followup.send(embed=e)

@bot.tree.command(name='release', description='Release a player from your squad', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.describe(player='The player to release')
@app_commands.default_permissions(manage_messages=True)
async def release_cmd(it, player: discord.Member):
    if not is_manager(it.user) and not it.user.guild_permissions.administrator:
        await it.response.send_message('Only managers / assistants can release players.', ephemeral=True); return
    mgr_team = get_manager_team(it.user)
    player_team = get_member_team(player)
    if not player_team:
        await it.response.send_message(f'{player.mention} is not on any team.', ephemeral=True); return
    if not it.user.guild_permissions.administrator:
        if mgr_team is None or mgr_team != player_team:
            await it.response.send_message(f'{player.mention} is not on **your** team.', ephemeral=True); return
    team_role = get_team_role(it.guild, player_team)
    if team_role:
        try: await player.remove_roles(team_role, reason=f'Released from {player_team}')
        except Exception as ex:
            await it.response.send_message(f'Failed to remove role: {ex}', ephemeral=True); return
    e = discord.Embed(color=C['d'], description=f'{player.mention} has been released from **{tfmt(player_team)}**.')
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.response.send_message(embed=e)
    try: await player.send(embed=discord.Embed(color=C['d'], description=f'You were released from **{tfmt(player_team)}** by {it.user.mention}.'))
    except: pass

@bot.tree.command(name='forceadd', description='[Admin] Force-add a player to a team', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.describe(player='Player to add', team='Target team')
@app_commands.choices(team=TEAM_CHOICES)
@app_commands.default_permissions(administrator=True)
async def forceadd_cmd(it, player: discord.Member, team: str):
    existing = get_member_team(player)
    if existing:
        await it.response.send_message(f'{player.mention} is already on **{tfmt(existing)}**.', ephemeral=True); return
    role = get_team_role(it.guild, team)
    if not role:
        await it.response.send_message('Team role not found in this server.', ephemeral=True); return
    try: await player.add_roles(role, reason=f'Force-added to {team} by {it.user}')
    except Exception as ex:
        await it.response.send_message(f'Failed to add role: {ex}', ephemeral=True); return
    e = discord.Embed(color=C['a'], description=f'{player.mention} force-added to **{tfmt(team)}** by {it.user.mention}.')
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.response.send_message(embed=e)

@bot.tree.command(name='teamsheet', description="View a nation's current squad", guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.choices(team=TEAM_CHOICES)
async def teamsheet_cmd(it: discord.Interaction, team: str):
    await it.response.defer()
    roster = get_team_roster(it.guild, team)
    e = discord.Embed(color=0x2b2d31, title=f"{tfmt(team)} — Squad Sheet")

    # Role IDs to exclude from the "roles" display (team roles, @everyone, known staff noise)
    EXCLUDED_ROLE_IDS = set(TEAM_ROLES.values()) | {
        it.guild.default_role.id,  # @everyone
        STAFF_ROLE_ID, REFEREE_ROLE_ID,
        MANAGER_ROLE_ID, ASST_ROLE_ID,
    }

    def member_roles_str(m: discord.Member) -> str:
        """Return a comma-separated list of visible non-team roles for a member."""
        roles = [
            r for r in m.roles
            if r.id not in EXCLUDED_ROLE_IDS and not r.managed  # skip bot-managed roles
        ]
        # Sort highest position first (most important role first)
        roles.sort(key=lambda r: r.position, reverse=True)
        return ', '.join(r.mention for r in roles) if roles else '—'

    if not roster:
        e.description = 'No players are signed to this nation yet.'
    else:
        mgrs  = [m for m in roster if MANAGER_ROLE_ID in {r.id for r in m.roles}]
        amgrs = [m for m in roster if ASST_ROLE_ID in {r.id for r in m.roles} and MANAGER_ROLE_ID not in {r.id for r in m.roles}]
        players = [m for m in roster if not is_manager(m)]

        lines = []

        if mgrs:
            lines.append('**— Manager —**')
            for m in mgrs:
                role_str = member_roles_str(m)
                lines.append(f'`[M]` {m.mention} **({m.display_name})** — {role_str}')
            lines.append('')

        if amgrs:
            lines.append('**— Assistant Manager —**')
            for m in amgrs:
                role_str = member_roles_str(m)
                lines.append(f'`[AM]` {m.mention} **({m.display_name})** — {role_str}')
            lines.append('')

        if players:
            lines.append('**— Players —**')
            for m in players:
                role_str = member_roles_str(m)
                lines.append(f'{m.mention} **({m.display_name})** — {role_str}')

        e.description = '\n'.join(lines)
        ft, fi = footer(it.guild)
        e.set_footer(text=f'RFA • {len(players)} player(s) | {len(mgrs) + len(amgrs)} staff', icon_url=fi)

    await it.followup.send(embed=e)

@bot.tree.command(name='freeagent', description='Post your free-agent ad in the free-agency channel', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.describe(position='Your position (e.g. GK, CB, ST)', experience='Your experience level', about='Short description (optional)')
async def freeagent_cmd(it, position: str, experience: str, about: str = None):
    if get_member_team(it.user):
        await it.response.send_message('You are already signed to a team. Release yourself first.', ephemeral=True); return
    last = get_fa_cooldown(it.guild_id, it.user.id)
    remaining = FREE_AGENT_COOLDOWN - (time.time() - last)
    if remaining > 0:
        hrs = int(remaining // 3600)
        mins = int((remaining % 3600) // 60)
        await it.response.send_message(f'You can post again in {hrs}h {mins}m.', ephemeral=True); return
    ch = it.guild.get_channel(FREE_AGENT_CHANNEL_ID)
    if not ch:
        await it.response.send_message('Free-agency channel not found.', ephemeral=True); return
    set_fa_cooldown(it.guild_id, it.user.id)
    e = discord.Embed(color=C['gold'], title='Free Agent Available')
    e.set_author(name=it.user.display_name, icon_url=it.user.display_avatar.url)
    e.add_field(name='Player', value=it.user.mention, inline=True)
    e.add_field(name='Position', value=position.upper(), inline=True)
    e.add_field(name='Experience', value=experience, inline=False)
    if about: e.add_field(name='About', value=about, inline=False)
    e.add_field(name='Interested?', value='DM this player or have your manager contact them directly.', inline=False)
    ft, fi = footer(it.guild)
    e.set_footer(text=f'Roblox Football Association • {datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M")} UTC', icon_url=fi)
    await ch.send(content=it.user.mention, embed=e)
    await it.response.send_message(f'Your free-agent post has been sent to {ch.mention}!', ephemeral=True)

@bot.tree.command(name='friendly', description='Request a friendly match', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
async def friendlies_cmd(it: discord.Interaction):
    # Resolve guild — command is guild-only but user may invoke from a DM via the guild's slash command list
    guild = it.guild or bot.get_guild(DISCORD_GUILD_ID)
    if not guild:
        await it.response.send_message('Could not find the RFA guild. Please use this command inside the server.', ephemeral=True)
        return

    friendly_role = guild.get_role(FRIENDLY_PING_ROLE_ID)
    if not friendly_role:
        await it.response.send_message('Friendly ping role not found. Contact an administrator.', ephemeral=True)
        return

    # Fetch the member object from the guild so role checks work even if invoked from DMs
    member = it.guild and it.user  # already a Member in guild context
    if not isinstance(member, discord.Member):
        member = guild.get_member(it.user.id)
    if not member:
        try:
            member = await guild.fetch_member(it.user.id)
        except discord.NotFound:
            await it.response.send_message('You must be a member of the RFA server to use this command.', ephemeral=True)
            return

    has_friendly_role = friendly_role in member.roles
    is_manager_or_assistant = is_manager(member) or member.guild_permissions.administrator

    if not (has_friendly_role or is_manager_or_assistant):
        await it.response.send_message('You need the designated friendly role or be a Manager/Assistant Manager to request friendlies.', ephemeral=True)
        return

    my_team = get_manager_team(member) if is_manager_or_assistant else None

    ch = bot.get_channel(FRIENDLIES_CHANNEL_ID)
    if not ch:
        await it.response.send_message('Friendlies channel not found.', ephemeral=True)
        return

    cooldown_key = f'rfa/{guild.id}/friendly_cooldown/{it.user.id}'
    last_used = _r(cooldown_key).get()
    if last_used and (time.time() - last_used) < 900:
        remaining = 900 - (time.time() - last_used)
        mins = int(remaining // 60)
        secs = int(remaining % 60)
        await it.response.send_message(f'You can use this command again in {mins}m {secs}s.', ephemeral=True)
        return
    _r(cooldown_key).set(time.time())

    # Plain display name shown next to the mention so it reads clearly in the channel
    plain_name = member.display_name

    if my_team:
        description = f'**{tfmt(my_team)}** is looking for a friendly. Contact {it.user.mention} ({plain_name}) to arrange a match.'
        fields = [
            ('Requested by', f'{it.user.mention} ({plain_name})', True),
            ('Nation', tfmt(my_team), True),
        ]
    else:
        description = f'A community member is looking for a friendly. Contact {it.user.mention} ({plain_name}) to arrange a match.'
        fields = [
            ('Requested by', f'{it.user.mention} ({plain_name})', True),
        ]

    e = discord.Embed(color=C['pr'], title='Friendly Match Request', description=description)
    for name, value, inline in fields:
        e.add_field(name=name, value=value, inline=inline)
    ft, fi = footer(guild)
    e.set_footer(text=f'Roblox Football Association • {datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M")} UTC', icon_url=fi)

    await ch.send(content=friendly_role.mention, embed=e)
    await it.response.send_message('Friendly request posted.', ephemeral=True)

@bot.tree.command(name='scout', description='Look for players as a team manager', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.describe(info='What kind of players or positions you are looking for')
async def scout_cmd(it, info: str):
    if not is_manager(it.user):
        await it.response.send_message('Only Managers and Assistant Managers can use this command.', ephemeral=True)
        return
    my_team = get_manager_team(it.user)
    if my_team is None:
        await it.response.send_message('You do not have a team role assigned. Contact an administrator.', ephemeral=True)
        return
    ch = bot.get_channel(SCOUT_CHANNEL_ID)
    if not ch:
        await it.response.send_message('Scout channel not found.', ephemeral=True)
        return
    cooldown_key = f'rfa/{it.guild_id}/scout_cooldown/{it.user.id}'
    last_used = _r(cooldown_key).get()
    if last_used and (time.time() - last_used) < 1800:
        remaining = 1800 - (time.time() - last_used)
        mins = int(remaining // 60)
        secs = int(remaining % 60)
        await it.response.send_message(f'You can use this command again in {mins}m {secs}s.', ephemeral=True)
        return
    _r(cooldown_key).set(time.time())
    e = discord.Embed(color=C['pr'], title='Scout Request')
    e.add_field(name='Team', value=tfmt(my_team), inline=True)
    e.add_field(name='Manager', value=it.user.mention, inline=True)
    e.add_field(name='Looking for', value=info, inline=False)
    ft, fi = footer(it.guild)
    e.set_footer(text=f'Roblox Football Association • {datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M")} UTC', icon_url=fi)
    await ch.send(embed=e)
    await it.response.send_message('Scout request posted.', ephemeral=True)

@bot.tree.command(name='massverify', description='Verify all unverified members who are linked on RoVer', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def mass_verify(it: discord.Interaction):
    UNVERIFIED_ROLE_ID = 1293659033487675432
    COMMUNITY_ROLE_ID = 1293658127522074637

    await it.response.defer()
    guild = it.guild
    unverified_role = guild.get_role(UNVERIFIED_ROLE_ID)
    community_role = guild.get_role(COMMUNITY_ROLE_ID)

    if not unverified_role:
        await it.followup.send('❌ Unverified role not found.'); return
    if not community_role:
        await it.followup.send('❌ Community role not found.'); return

    unverified_members = [m for m in guild.members if unverified_role in m.roles]
    if not unverified_members:
        await it.followup.send('✅ No unverified members found.'); return

    await it.followup.send(f'🔄 Scanning **{len(unverified_members)}** unverified members... This will take a while, do not run the command again.')

    verified_count = 0
    not_verified_count = 0
    failed_count = 0
    nickname_failed_count = 0

    async with aiohttp.ClientSession(headers={'Authorization': 'Bearer rvr2g09tiy3u0peyo032wyvsoo8ce3k9xtisx71x8n3jh64a5dxgrl61sunyqfnbnqyu'}) as session:
        for member in unverified_members:
            for attempt in range(5):
                try:
                    async with session.get(f'https://registry.rover.link/api/guilds/{guild.id}/discord-to-roblox/{member.id}') as response:
                        remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
                        reset_after = float(response.headers.get('X-RateLimit-Reset-After', 1))

                        if response.status == 200:
                            data = await response.json()
                            roblox_username = data.get('cachedUsername', 'Unknown')

                            # Update roles
                            await member.remove_roles(unverified_role, reason='Mass verify')
                            await member.add_roles(community_role, reason='Mass verify')

                            # Apply Roblox username as nickname
                            try:
                                await member.edit(nick=roblox_username, reason='Mass verify — RoVer sync')
                            except discord.Forbidden:
                                # Bot can't rename members with equal/higher roles (e.g. admins)
                                nickname_failed_count += 1
                                print(f'[massverify] Cannot rename {member} — insufficient permissions')
                            except discord.HTTPException as e:
                                nickname_failed_count += 1
                                print(f'[massverify] Failed to rename {member}: {e}')

                            verified_count += 1
                            print(f'Verified {member} → {roblox_username}')

                            if remaining == 0:
                                print(f'Bucket exhausted, waiting {reset_after}s...')
                                await asyncio.sleep(reset_after + 0.5)
                            break

                        elif response.status == 404:
                            not_verified_count += 1
                            if remaining == 0:
                                print(f'Bucket exhausted, waiting {reset_after}s...')
                                await asyncio.sleep(reset_after + 0.5)
                            break

                        elif response.status == 429:
                            retry_after = float(response.headers.get('Retry-After', 60))
                            print(f'429 on {member}, waiting {retry_after}s before retrying... (attempt {attempt + 1}/5)')
                            await asyncio.sleep(retry_after + 1)

                        else:
                            print(f'Unexpected {response.status} on {member}')
                            failed_count += 1
                            break

                except Exception as e:
                    print(f'Error on {member}: {e.__class__.__name__}: {e}')
                    await asyncio.sleep(5)
            else:
                failed_count += 1
                print(f'Gave up on {member} after 5 attempts')

            await asyncio.sleep(1.0)

    e = discord.Embed(title='Mass Verify Complete', color=C['a'])
    e.add_field(name='✅ Verified', value=str(verified_count), inline=True)
    e.add_field(name='⏭️ Not on RoVer', value=str(not_verified_count), inline=True)
    e.add_field(name='❌ Errors', value=str(failed_count), inline=True)
    e.add_field(name='⚠️ Nickname Skipped', value=str(nickname_failed_count), inline=True)
    e.set_footer(text=f'Scanned {len(unverified_members)} members total')
    await it.edit_original_response(content=None, embed=e)
    


@bot.tree.command(name='signing', description='Toggle the signing window open or closed', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.choices(status=[app_commands.Choice(name='Open', value=1), app_commands.Choice(name='Closed', value=0)])
@app_commands.default_permissions(administrator=True)
async def signing_cmd(it, status: int):
    set_signing(it.guild_id, bool(status))
    e = discord.Embed(color=C['a'] if status else C['d'], description=f'Signing window is now **{"Open 🟢" if status else "Closed 🔴"}**.')
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.response.send_message(embed=e)

@bot.tree.command(name='config', description='Configure bot settings', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def config_cmd(it, signing_open_flag: bool = None, max_players: int = None,
                     ticket_category: discord.CategoryChannel = None, ticket_log: discord.TextChannel = None,
                     manager_role: discord.Role = None, assistant_manager_role: discord.Role = None):
    updates = {}
    if signing_open_flag is not None: updates['open'] = int(signing_open_flag)
    if max_players: updates['maxp'] = max_players
    if ticket_category: updates['tcat'] = ticket_category.id
    if ticket_log: updates['tlog'] = ticket_log.id
    if manager_role: updates['mgr_role'] = manager_role.id
    if assistant_manager_role: updates['amgr_role'] = assistant_manager_role.id
    if updates: _r(f'rfa/{it.guild_id}/cfg').update(updates)
    cfg = _r(f'rfa/{it.guild_id}/cfg').get() or {}
    e = discord.Embed(title='Server Configuration', color=C['pr'])
    e.add_field(name='Signing', value='Open 🟢' if cfg.get('open', 1) else 'Closed 🔴', inline=True)
    e.add_field(name='Max Players/Squad', value=str(cfg.get('maxp', 25)), inline=True)
    e.add_field(name='Ticket Category', value=f'<#{cfg["tcat"]}>' if cfg.get('tcat') else 'Not set', inline=True)
    e.add_field(name='Ticket Log', value=f'<#{cfg["tlog"]}>' if cfg.get('tlog') else 'Not set', inline=True)
    e.add_field(name='Manager Role', value=f'<@&{cfg["mgr_role"]}>' if cfg.get('mgr_role') else 'Not set', inline=True)
    e.add_field(name='Asst. Mgr Role', value=f'<@&{cfg["amgr_role"]}>' if cfg.get('amgr_role') else 'Not set', inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name='ticket', description='Post the ticket panel in this channel', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def ticket_panel_cmd(it):
    tcat = _r(f'rfa/{it.guild_id}/cfg/tcat').get()
    if not tcat:
        await it.response.send_message('Set a ticket category first with `/config`.', ephemeral=True); return
    e = discord.Embed(color=C['pr'], description='Click below to open a support ticket. A private channel will be created for you.')
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.channel.send(embed=e, view=TicketPanelView())
    await it.response.send_message('Ticket panel posted.', ephemeral=True)

@bot.tree.command(name='addtoticket', description='Add a member to the current ticket channel', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.describe(member='The member to add to this ticket')
async def addtoticket_cmd(it: discord.Interaction, member: discord.Member):
    # Must be used inside a ticket channel
    tk = _r(f'rfa/{it.guild_id}/tickets/{it.channel_id}').get()
    if not tk:
        await it.response.send_message('This command can only be used inside a ticket channel.', ephemeral=True)
        return

    # Permission check: ticket owner OR staff/admin
    can_add = (
        it.user.id == tk['uid']
        or it.user.guild_permissions.manage_channels
        or it.user.guild_permissions.administrator
        or is_staff(it.user)
    )
    if not can_add:
        await it.response.send_message('Only the ticket owner or staff can add members to this ticket.', ephemeral=True)
        return

    # Don't add bots
    if member.bot:
        await it.response.send_message('You cannot add bots to tickets.', ephemeral=True)
        return

    # Check if they already have access
    channel = it.channel
    overwrites = channel.overwrites
    existing = overwrites.get(member)
    if existing and existing.view_channel:
        await it.response.send_message(f'{member.mention} already has access to this ticket.', ephemeral=True)
        return

    # Grant view + send access
    try:
        await channel.set_permissions(
            member,
            view_channel=True,
            send_messages=True,
            attach_files=True,
            reason=f'Added to ticket by {it.user}',
        )
    except discord.Forbidden:
        await it.response.send_message('I do not have permission to edit this channel.', ephemeral=True)
        return

    ft, fi = footer(it.guild)
    e = discord.Embed(
        color=C['a'],
        description=f'{member.mention} has been added to this ticket by {it.user.mention}.',
    )
    e.set_footer(text=ft, icon_url=fi)
    await it.response.send_message(embed=e)


@bot.tree.command(name='renameticket', description='Rename the current ticket channel', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.describe(name='New name for the ticket channel (no spaces — use hyphens)')
async def renameticket_cmd(it: discord.Interaction, name: str):
    # Must be used inside a ticket channel
    tk = _r(f'rfa/{it.guild_id}/tickets/{it.channel_id}').get()
    if not tk:
        await it.response.send_message('This command can only be used inside a ticket channel.', ephemeral=True)
        return

    # Permission check: ticket owner OR staff/admin
    can_rename = (
        it.user.id == tk['uid']
        or it.user.guild_permissions.manage_channels
        or it.user.guild_permissions.administrator
        or is_staff(it.user)
    )
    if not can_rename:
        await it.response.send_message('Only the ticket owner or staff can rename this ticket.', ephemeral=True)
        return

    # Sanitise: lowercase, replace spaces with hyphens, strip bad chars
    safe_name = name.lower().replace(' ', '-')[:100]

    old_name = it.channel.name
    try:
        await it.channel.edit(name=safe_name, reason=f'Ticket renamed by {it.user}')
    except discord.Forbidden:
        await it.response.send_message('I do not have permission to rename this channel.', ephemeral=True)
        return
    except discord.HTTPException as ex:
        await it.response.send_message(f'Rename failed: {ex}', ephemeral=True)
        return

    ft, fi = footer(it.guild)
    e = discord.Embed(
        color=C['pr'],
        description=f'Ticket renamed from `{old_name}` → `{safe_name}` by {it.user.mention}.',
    )
    e.set_footer(text=ft, icon_url=fi)
    await it.response.send_message(embed=e)


async def roblox_get_server_players(server_id: str) -> list[str]:
    """Fetch the list of player usernames in a specific Roblox server via Presence API."""
    try:
        # Thumbnail/presence APIs don't expose per-server player lists publicly.
        # The Game Server Presence endpoint requires the server token — we skip silently.
        return []
    except Exception:
        return []


async def roblox_get_place_name(place_id: str) -> str | None:
    """Fetch the place display name from the Roblox Games API."""
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f'https://games.roblox.com/v1/games/multiget-place-details?placeIds={place_id}') as r:
                if r.status != 200:
                    return None
                data = await r.json()
                if data:
                    return data[0].get('name')
    except Exception:
        return None
    return None


@bot.tree.command(name='ticketstats', description='Show ticket counts and move a ticket to a different category', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(manage_channels=True)
async def ticketstats_cmd(it: discord.Interaction):
    """Shows open/closed ticket counts. Staff only."""
    await it.response.defer(ephemeral=True)
    tickets = _r(f'rfa/{it.guild_id}/tickets').get() or {}

    open_tickets   = [tid for tid, tk in tickets.items() if tk.get('status') == 'open']
    closed_tickets = [tid for tid, tk in tickets.items() if tk.get('status') == 'closed']

    # Break open tickets down by reason
    by_reason: dict[str, int] = {}
    for tid in open_tickets:
        reason = tickets[tid].get('reason', 'Unknown')
        by_reason[reason] = by_reason.get(reason, 0) + 1

    e = discord.Embed(title='Ticket Statistics', color=C['pr'])
    e.add_field(name='🟢 Open',   value=str(len(open_tickets)),   inline=True)
    e.add_field(name='🔴 Closed', value=str(len(closed_tickets)), inline=True)
    e.add_field(name='📋 Total',  value=str(len(tickets)),        inline=True)

    if by_reason:
        breakdown = '\n'.join(f'`{reason}` — {count}' for reason, count in sorted(by_reason.items()))
        e.add_field(name='Open Tickets by Reason', value=breakdown, inline=False)

    # List active ticket channels
    if open_tickets:
        ch_lines = []
        for tid in open_tickets[:10]:
            ch = it.guild.get_channel(int(tid))
            opener = tickets[tid].get('uid')
            ch_lines.append(f'{ch.mention if ch else f"`#{tid}`"} — opened by <@{opener}>' if opener else (ch.mention if ch else f'`#{tid}`'))
        if len(open_tickets) > 10:
            ch_lines.append(f'… and {len(open_tickets) - 10} more')
        e.add_field(name='Active Tickets', value='\n'.join(ch_lines), inline=False)

    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e, ephemeral=True)


@bot.tree.command(name='moveticket', description='Move the current ticket to a different category', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.describe(category='The category to move this ticket into')
async def moveticket_cmd(it: discord.Interaction, category: discord.CategoryChannel):
    # Must be used inside a ticket channel
    tk = _r(f'rfa/{it.guild_id}/tickets/{it.channel_id}').get()
    if not tk:
        await it.response.send_message('This command can only be used inside a ticket channel.', ephemeral=True)
        return

    # Staff / admin only for moving
    can_move = (
        it.user.guild_permissions.manage_channels
        or it.user.guild_permissions.administrator
        or is_staff(it.user)
    )
    if not can_move:
        await it.response.send_message('Only staff can move ticket channels.', ephemeral=True)
        return

    old_category = it.channel.category
    try:
        await it.channel.edit(category=category, reason=f'Ticket moved by {it.user}')
    except discord.Forbidden:
        await it.response.send_message('I do not have permission to move this channel.', ephemeral=True)
        return
    except discord.HTTPException as ex:
        await it.response.send_message(f'Move failed: {ex}', ephemeral=True)
        return

    ft, fi = footer(it.guild)
    old_name = old_category.name if old_category else 'Unknown'
    e = discord.Embed(
        color=C['pr'],
        description=f'Ticket moved from **{old_name}** → **{category.name}** by {it.user.mention}.',
    )
    e.set_footer(text=ft, icon_url=fi)
    await it.response.send_message(embed=e)


@bot.tree.command(name='serverstatus', description='Check live player count and servers', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
async def serverstatus_cmd(it: discord.Interaction):
    await it.response.defer()
    player_count = await roblox_get_player_count()
    servers = await roblox_get_servers()

    # Try to get the place/game name for a nicer title
    place_name = await roblox_get_place_name(ROBLOX_UNIVERSE) or 'RFA Universe'

    e = discord.Embed(title=f'{place_name} — Server Status', color=C['pr'])
    e.add_field(name='👥 Players Online', value=str(player_count) if player_count is not None else 'Unavailable', inline=True)
    e.add_field(name='🖥️ Active Servers', value=str(len(servers)), inline=True)

    if servers:
        server_lines = []
        for i, sv in enumerate(servers[:5], start=1):
            playing   = sv.get('playing', 0)
            max_p     = sv.get('maxPlayers', '?')
            ping      = sv.get('ping', '?')
            server_id = sv.get('id', '')

            # Roblox's public API doesn't expose per-server player names without a server token,
            # so we show the count + ping cleanly.
            line = f'**Server {i}** — `{playing}/{max_p}` players | ping `{ping}ms`'
            if server_id:
                line += f'\n  └ ID: `{server_id[:16]}…`'
            server_lines.append(line)

        e.add_field(
            name='📋 Server List (top 5)',
            value='\n'.join(server_lines),
            inline=False,
        )

        # Best-effort: show total slots
        total_slots = sum(sv.get('maxPlayers', 0) for sv in servers)
        total_playing = sum(sv.get('playing', 0) for sv in servers)
        if total_slots:
            e.add_field(name='📊 Capacity', value=f'{total_playing}/{total_slots} slots filled across {len(servers)} server(s)', inline=False)

    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)

@bot.tree.command(name='rban', description='Ban a player from the Roblox game', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def rban_cmd(it, username: str, reason: str, duration_days: int = None):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.'); return
    success, msg = await roblox_ban(user_id, reason, duration_days)
    if not success:
        await it.followup.send(f'Ban failed: `{msg}`'); return
    dur = f'{duration_days} day(s)' if duration_days else 'Permanent'
    _r(f'rfa/{it.guild_id}/roblox_bans/{user_id}').set({'username':username,'reason':reason,'duration_days':duration_days,'permanent':duration_days is None,'banned_by':it.user.id,'banned_at':_now()})
    audit_log(it.guild_id, 'rban', {'username':username,'user_id':user_id,'reason':reason,'duration':dur,'by':it.user.name,'by_id':it.user.id})
    await roblox_message('ChatLog', {'scope':'all','color':'red','text':f'[BAN] {username} banned. Reason: {reason} ({dur})','sender':'RFA System'})
    e = discord.Embed(color=C['d'], title='Roblox Ban Issued')
    e.add_field(name='Username', value=username, inline=True)
    e.add_field(name='User ID', value=str(user_id), inline=True)
    e.add_field(name='Duration', value=dur, inline=True)
    e.add_field(name='Reason', value=reason, inline=False)
    e.add_field(name='Banned by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)

@bot.tree.command(name='runban', description='Unban a player from the Roblox game', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def runban_cmd(it, username: str):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.'); return
    success, msg = await roblox_unban(user_id)
    if not success:
        await it.followup.send(f'Unban failed: `{msg}`'); return
    _r(f'rfa/{it.guild_id}/roblox_bans/{user_id}').delete()
    audit_log(it.guild_id, 'runban', {'username':username,'user_id':user_id,'by':it.user.name,'by_id':it.user.id})
    await roblox_message('ChatLog', {'scope':'all','color':'green','text':f'[UNBAN] {username} unbanned.','sender':'RFA System'})
    e = discord.Embed(color=C['a'], title='Roblox Ban Removed')
    e.add_field(name='Username', value=username, inline=True)
    e.add_field(name='Unbanned by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)

@bot.tree.command(name='rbaninfo', description='Check ban status of a Roblox player', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def rbaninfo_cmd(it, username: str):
    await it.response.defer(ephemeral=True)
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.', ephemeral=True); return
    data = await roblox_get_ban(user_id)
    restriction = data.get('gameJoinRestriction', {}) if data else {}
    if not restriction.get('active', False):
        e = discord.Embed(color=C['a'], title='Not Banned', description=f'**{username}** has no active ban.')
        e.add_field(name='User ID', value=str(user_id), inline=True)
        await it.followup.send(embed=e, ephemeral=True); return
    duration = restriction.get('duration')
    dur_str = f"{int(duration.rstrip('s')) // 86400} day(s)" if duration else 'Permanent'
    fb_data = _r(f'rfa/{it.guild_id}/roblox_bans/{user_id}').get() or {}
    e = discord.Embed(color=C['d'], title='Player is Banned')
    e.add_field(name='Username', value=username, inline=True)
    e.add_field(name='User ID', value=str(user_id), inline=True)
    e.add_field(name='Duration', value=dur_str, inline=True)
    e.add_field(name='Reason', value=restriction.get('displayReason', 'No reason'), inline=False)
    e.add_field(name='Banned by', value=f'<@{fb_data["banned_by"]}>' if fb_data.get('banned_by') else 'Unknown', inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e, ephemeral=True)

@bot.tree.command(name='rbans', description='List all currently banned Roblox players', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def rbans_cmd(it):
    await it.response.defer(ephemeral=True)
    bans = await roblox_get_all_bans()
    if not bans:
        await it.followup.send('No active bans found.', ephemeral=True); return
    e = discord.Embed(title=f'Active Bans ({len(bans)})', color=C['d'])
    lines = []
    for ban in bans[:20]:
        uid = ban.get('user', '').split('/')[-1]
        restriction = ban.get('gameJoinRestriction', {})
        duration = restriction.get('duration')
        dur_str = f"{int(duration.rstrip('s')) // 86400}d" if duration else 'Perm'
        fb = _r(f'rfa/{it.guild_id}/roblox_bans/{uid}').get() or {}
        name = fb.get('username', f'ID:{uid}')
        reason = restriction.get('displayReason', 'No reason')[:40]
        lines.append(f'`{name}` — {dur_str} — {reason}')
    e.description = '\n'.join(lines)
    ft, fi = footer(it.guild)
    e.set_footer(text=f'Showing {min(len(bans),20)} of {len(bans)} | RFA', icon_url=fi)
    await it.followup.send(embed=e, ephemeral=True)

ANNOUNCE_COLORS = [app_commands.Choice(name=n, value=n.lower()) for n in ['White','Red','Green','Blue','Yellow','Orange','Purple','Cyan','Pink']]
DISCORD_COLOR_MAP = {'white':0xffffff,'red':0xed4245,'green':0x57f287,'blue':0x5865f2,'yellow':0xfee75c,'orange':0xfaa61a,'purple':0x9b59b6,'cyan':0x1abc9c,'pink':0xff69b4}

@bot.tree.command(name='announce', description='Broadcast a message into the Roblox game', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.choices(color=ANNOUNCE_COLORS)
@app_commands.default_permissions(administrator=True)
async def announce_cmd(it, message: str, color: str = 'white', topic: str = 'Announcements'):
    await it.response.defer()
    success, msg = await roblox_announce(topic, json.dumps({'text':message,'sender':it.user.name,'color':color}))
    if not success:
        await it.followup.send(f'Announce failed: `{msg}`'); return
    audit_log(it.guild_id, 'announce', {'message':message,'color':color,'topic':topic,'by':it.user.name,'by_id':it.user.id})
    e = discord.Embed(color=DISCORD_COLOR_MAP.get(color, 0xffffff), title='Announcement Sent')
    e.add_field(name='Message', value=message, inline=False)
    e.add_field(name='Color', value=color.title(), inline=True)
    e.add_field(name='Topic', value=f'`{topic}`', inline=True)
    e.add_field(name='Sent by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)

@bot.tree.command(name='mod', description='Give a player mod in a specific Roblox server', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def mod_cmd(it, server: str, username: str):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.'); return
    await ds_set(f'mod_{user_id}', {'type':'server','server':server,'username':username,'userId':user_id,'granted_by':it.user.name,'granted_at':_now()})
    await kohl_set_power(user_id, 2)
    await roblox_message('ModSystem', {'action':'grant','userId':user_id,'username':username,'modType':'server','server':server})
    audit_log(it.guild_id, 'mod', {'username':username,'user_id':user_id,'server':server,'type':'server','by':it.user.name,'by_id':it.user.id})
    e = discord.Embed(color=C['a'], title='Server Mod Granted')
    e.add_field(name='Username', value=username, inline=True)
    e.add_field(name='User ID', value=str(user_id), inline=True)
    e.add_field(name='Server', value=server, inline=True)
    e.add_field(name='Granted by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)

@bot.tree.command(name='permmod', description='Give a player permanent mod across all servers', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def permmod_cmd(it, username: str):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.'); return
    await ds_set(f'mod_{user_id}', {'type':'permanent','username':username,'userId':user_id,'granted_by':it.user.name,'granted_at':_now()})
    await kohl_set_power(user_id, -3)
    await roblox_message('ModSystem', {'action':'grant','userId':user_id,'username':username,'modType':'permanent'})
    audit_log(it.guild_id, 'permmod', {'username':username,'user_id':user_id,'type':'permanent','by':it.user.name,'by_id':it.user.id})
    e = discord.Embed(color=C['pr'], title='Permanent Mod Granted')
    e.add_field(name='Username', value=username, inline=True)
    e.add_field(name='User ID', value=str(user_id), inline=True)
    e.add_field(name='Scope', value='All Servers', inline=True)
    e.add_field(name='Granted by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)

@bot.tree.command(name='unmod', description='Remove mod from a Roblox player', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def unmod_cmd(it, username: str):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.'); return
    ds_ok, ds_msg = await ds_delete(f'mod_{user_id}')
    ds_note = ''
    if not ds_ok:
        if '404' in ds_msg or 'NOT_FOUND' in ds_msg: ds_note = 'No DataStore entry — was modded in-game via Kohl'
        else:
            await it.followup.send(f'DataStore error: `{ds_msg}`'); return
    ms_ok, ms_msg = await roblox_message('ModSystem', {'action':'revoke','userId':user_id,'username':username})
    if not ms_ok:
        await it.followup.send(f'MessagingService failed: `{ms_msg}`'); return
    await kohl_set_power(user_id, 0)
    audit_log(it.guild_id, 'unmod', {'username':username,'user_id':user_id,'by':it.user.name,'by_id':it.user.id,'note':ds_note or None})
    await roblox_message('ChatLog', {'scope':'all','color':'red','text':f'[UNMOD] {username} mod removed.','sender':'RFA System'})
    e = discord.Embed(color=C['d'], title='Mod Removed')
    e.add_field(name='Username', value=username, inline=True)
    e.add_field(name='User ID', value=str(user_id), inline=True)
    e.add_field(name='Removed by', value=it.user.mention, inline=True)
    if ds_note: e.add_field(name='Note', value=ds_note, inline=False)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)

@bot.tree.command(name='modlist', description='List all mods/admins from DataStore and Kohl', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def modlist_cmd(it):
    await it.response.defer(ephemeral=True)
    ds_keys = await ds_list(prefix='mod_')
    kohl_admins = await kohl_read()
    unified = {}
    for entry in ds_keys:
        data = await ds_get(entry.get('key', ''))
        if not data: continue
        uid = data.get('userId')
        if not uid: continue
        unified[uid] = {'username':data.get('username',str(uid)),'power':2 if data.get('type')=='server' else 3,'source':'Discord','detail':f'Server: {data.get("server","?")}' if data.get('type')=='server' else 'Permanent'}
    for uid, power in kohl_admins.items():
        abs_power = abs(power)
        if abs_power < 1: continue
        username = unified.get(uid, {}).get('username') or await kohl_get_username(uid)
        unified[uid] = {'username':username,'power':abs_power,'source':'Both' if uid in unified else 'Kohl','detail':'Permanent' if power < 0 else 'Temporary'}
    if not unified:
        await it.followup.send('No mods or admins found.', ephemeral=True); return
    lines = [f'**{d["username"]}** — Level {d["power"]} ({d["detail"]}) `[{d["source"]}]`'
             for _, d in sorted(unified.items(), key=lambda x: x[1]['power'], reverse=True)]
    e = discord.Embed(title='Staff List', color=C['pr'], description='\n'.join(lines))
    e.set_footer(text=f'{len(lines)} total | Roblox Football Association')
    await it.followup.send(embed=e, ephemeral=True)

@bot.tree.command(name='setpower', description="Set a Roblox user's power level in Kohl (0-6)", guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def setpower_cmd(it, username: str, power: int, permanent: bool = True):
    if not 0 <= power <= 6:
        await it.response.send_message('Power must be between 0 and 6.', ephemeral=True); return
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.'); return
    ok, msg = await kohl_set_power(user_id, (-power if permanent else power) if power > 0 else 0)
    if not ok:
        await it.followup.send(f'Failed to update KSave: `{msg}`'); return
    await roblox_message('ModSystem', {'action':'grant' if power > 0 else 'revoke','userId':user_id,'username':username,'power':power})
    audit_log(it.guild_id, 'setpower', {'username':username,'user_id':user_id,'power':power,'permanent':permanent,'by':it.user.name,'by_id':it.user.id})
    e = discord.Embed(color=C['d'] if power == 0 else C['a'], title='Power Removed' if power == 0 else 'Power Set')
    e.add_field(name='Username', value=username, inline=True)
    e.add_field(name='User ID', value=str(user_id), inline=True)
    e.add_field(name='Power', value=f'{power} — {KOHL_TITLES.get(power,"Removed")}', inline=True)
    if power > 0: e.add_field(name='Type', value='Permanent' if permanent else 'Temporary', inline=True)
    e.add_field(name='Set by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)

@bot.tree.command(name='whois', description='Look up a Roblox user and their moderation history', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def whois_cmd(it, username: str):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.'); return
    info, ban_data, mod_ds = await roblox_get_user_info(user_id), await roblox_get_ban(user_id), await ds_get(f'mod_{user_id}')
    restriction = ban_data.get('gameJoinRestriction', {}) if ban_data else {}
    ban_str = 'None'
    if restriction.get('active', False):
        duration = restriction.get('duration')
        ban_str = f'{"Permanent" if not duration else f"{int(duration.rstrip(chr(115))) // 86400}d"} — {restriction.get("displayReason","No reason")}'
    mod_str = 'None'
    if mod_ds:
        if mod_ds.get('type') == 'permanent': mod_str = f'Permanent (by {mod_ds.get("granted_by","?")} on {mod_ds.get("granted_at","?")[:10]})'
        else: mod_str = f'Server: `{mod_ds.get("server","?")}` (by {mod_ds.get("granted_by","?")})'
    all_logs = _r(f'rfa/{it.guild_id}/audit_log').get() or {}
    history = []
    for entry in all_logs.values():
        if str(entry.get('username','')).lower() == username.lower() or str(entry.get('user_id','')) == str(user_id):
            ts = entry.get('timestamp','')[:16].replace('T',' ')
            detail = entry.get('reason') or entry.get('server') or entry.get('type') or ''
            history.append(f'`{ts}` **{entry.get("action","?").upper()}** by {entry.get("by","?")}' + (f' — {detail}' if detail else ''))
    e = discord.Embed(color=C['c'], title=f'Whois: {username}')
    e.add_field(name='Display Name', value=info.get('displayName',username) if info else username, inline=True)
    e.add_field(name='User ID', value=str(user_id), inline=True)
    e.add_field(name='Joined Roblox', value=info.get('created','')[:10] if info else 'Unknown', inline=True)
    e.add_field(name='Universe Ban', value=ban_str, inline=False)
    e.add_field(name='Mod Status', value=mod_str, inline=False)
    if info and info.get('description'): e.add_field(name='Bio', value=info['description'][:200], inline=False)
    e.add_field(name=f'Log History ({len(history)} entries)', value='\n'.join(history[-10:]) if history else 'No entries found.', inline=False)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)

@bot.tree.command(name='logs', description='View the moderation audit log', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def logs_cmd(it, action: str = None, limit: int = 20):
    await it.response.defer(ephemeral=True)
    limit = min(max(limit, 1), 50)
    all_logs = _r(f'rfa/{it.guild_id}/audit_log').get() or {}
    entries = sorted(all_logs.values(), key=lambda x: x.get('timestamp',''), reverse=True)
    if action: entries = [e for e in entries if e.get('action','').lower() == action.lower()]
    entries = entries[:limit]
    if not entries:
        await it.followup.send('No log entries found.', ephemeral=True); return
    lines = []
    for entry in entries:
        ts = entry.get('timestamp','')[:16].replace('T',' ')
        target = entry.get('username') or entry.get('message','')
        detail = entry.get('reason') or entry.get('server') or entry.get('type') or entry.get('color') or ''
        line = f'`{ts}` **{entry.get("action","?").upper()}**'
        if target: line += f' — `{target}`'
        if entry.get('by'): line += f' by {entry["by"]}'
        if detail: line += f' — {detail}'
        lines.append(line)
    e = discord.Embed(title='Audit Log' + (f' — {action.upper()}' if action else ''), color=C['c'], description='\n'.join(lines))
    ft, fi = footer(it.guild)
    e.set_footer(text=f'Showing {len(entries)} entries | RFA', icon_url=fi)
    await it.followup.send(embed=e, ephemeral=True)

class LinksView(discord.ui.View):
    def __init__(self, member_role_ids: set):
        super().__init__(timeout=120)
        self.add_item(LinksSelect(member_role_ids))


class LinksSelect(discord.ui.Select):
    def __init__(self, member_role_ids: set):
        options = []
        for label, description, url, required_roles in LINKS_DATA:
            if required_roles and not any(rid in member_role_ids for rid in required_roles):
                continue
            options.append(discord.SelectOption(label=label, description=description, value=url))

        super().__init__(
            placeholder='Choose a link…',
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, it: discord.Interaction):
        # Only the person who called /links can interact — URL is sent privately
        await it.response.send_message(self.values[0], ephemeral=True)


@bot.tree.command(
    name='links',
    description='Get a quick link for RFA',
    guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))),
)
async def links_cmd(it: discord.Interaction):
    # Works in both guild channels and DMs.
    # The dropdown is shown publicly so others know what links exist,
    # but selecting a link sends the URL only to the person who clicked.
    if it.guild and isinstance(it.user, discord.Member):
        member_role_ids = {r.id for r in it.user.roles}
    else:
        # In DMs — fetch guild membership to check role-gated links
        guild = bot.get_guild(DISCORD_GUILD_ID)
        member_role_ids = set()
        if guild:
            try:
                gm = guild.get_member(it.user.id) or await guild.fetch_member(it.user.id)
                member_role_ids = {r.id for r in gm.roles}
            except (discord.NotFound, discord.HTTPException):
                pass

    view = LinksView(member_role_ids)
    if not view.children or not view.children[0].options:
        await it.response.send_message('No links available here.', ephemeral=True)
        return

    # Send publicly so everyone in the channel can see and learn about the links menu.
    # The actual URLs are delivered privately when someone selects an option.
    await it.response.send_message(
        f'**RFA Links** — {it.user.mention} opened the links menu.\n'
        '> Select an option below to receive that link privately.',
        view=view,
    )

@bot.tree.command(name='addpin', description='Upload an image as a pin and grant it to a Roblox player', guild=discord.Object(id=int(os.environ.get('DISCORD_GUILD_ID', 0))))
@app_commands.default_permissions(administrator=True)
async def addpin_cmd(it, roblox_username: str, image: discord.Attachment):
    if not image.content_type or not image.content_type.startswith('image/'):
        await it.response.send_message('Please attach a valid image (PNG, JPG, etc.)', ephemeral=True); return
    await it.response.defer()
    roblox_user_id = await roblox_get_user_id(roblox_username)
    if not roblox_user_id:
        await it.followup.send(f'Roblox user **{roblox_username}** not found.'); return
    async with aiohttp.ClientSession() as s:
        async with s.get(image.url) as r:
            if r.status != 200:
                await it.followup.send('Failed to download the image from Discord.'); return
            image_bytes = await r.read()
    await it.followup.send(f'Uploading pin for **{roblox_username}**… this may take ~30 seconds.')
    success, result = await upload_image_to_roblox(image_bytes, image.filename, f'{roblox_username} Pin')
    if not success:
        await it.edit_original_response(content=f'Upload failed: {result}'); return
    ok, asset_id = await poll_asset_operation(result)
    if not ok:
        await it.edit_original_response(content=f'{asset_id}'); return
    granted, grant_msg = await grant_pin_to_player(roblox_user_id, asset_id)
    if not granted:
        await it.edit_original_response(content=f'✅ Image uploaded (ID: `{asset_id}`) but DataStore grant failed: {grant_msg}'); return
    audit_log(it.guild_id, 'addpin', {'roblox_username':roblox_username,'roblox_user_id':roblox_user_id,'asset_id':asset_id,'by':it.user.name,'by_id':it.user.id})
    e = discord.Embed(color=C['a'], title='Pin Granted')
    e.add_field(name='Player', value=roblox_username, inline=True)
    e.add_field(name='Roblox ID', value=str(roblox_user_id), inline=True)
    e.add_field(name='Asset ID', value=f'`{asset_id}`', inline=True)
    e.add_field(name='rbxassetid', value=f'`rbxassetid://{asset_id}`', inline=False)
    e.add_field(name='Granted by', value=it.user.mention, inline=True)
    e.set_thumbnail(url=image.url)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.edit_original_response(content=None, embed=e)

async def handle_check_member(request):
    try: data = await request.json()
    except: return web.json_response({'verified': False}, status=400)
    if data.get('secret') != ROVER_SECRET:
        return web.json_response({'verified': False}, status=403)
    username = data.get('username', '').lower().strip()
    if not username: return web.json_response({'verified': False})
    guild = bot.get_guild(DISCORD_GUILD_ID)
    if not guild: return web.json_response({'verified': False})
    for member in guild.members:
        if member.nick and member.nick.lower() == username:
            return web.json_response({'verified': True})
    return web.json_response({'verified': False})

async def start_web_server():
    app = web.Application()
    app.router.add_post('/check-member', handle_check_member)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', 8080).start()
    print('Web server running on port 8080')

GUILD_OBJ = discord.Object(id=DISCORD_GUILD_ID)

@bot.event
async def on_ready():
    print(f'Online: {bot.user}')
    try:
        all_rfa = _r('rfa').get() or {}
        for gid_str, gdata in all_rfa.items():
            for cid, row in (gdata.get('contracts') or {}).items():
                if row.get('status') == 'Pending':
                    bot.add_view(SignView(cid, int(gid_str), row.get('sg_id', 0)))
    except Exception as e: print(f'[on_ready] {e}')
    bot.add_view(CloseTicketView())
    bot.add_view(TicketPanelView())
    if not expire_loop.is_running(): expire_loop.start()
    await start_web_server()
    try:
        bot.tree.clear_commands(guild=None)
        await bot.tree.sync()
        synced = await bot.tree.sync(guild=GUILD_OBJ)
        print(f'Synced {len(synced)} commands to guild')
    except Exception as ex: print(f'Sync error: {ex}')

if __name__ == '__main__':
    bot.run(BOT_TOKEN)
