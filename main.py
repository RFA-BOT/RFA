import discord, os, random, io, asyncio, json, time
import aiohttp
from discord.ext import commands, tasks
from discord import app_commands
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, db as fdb
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()  # loads .env when running locally; Railway injects vars directly

#  intents 
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='!', intents=intents)

#  colours 
C = {'a': 0x57f287, 'd': 0xed4245, 'c': 0x4f545c, 'pr': 0x5865f2, 'gold': 0xf1c40f}

#  team → role ID map 
TEAM_ROLES: dict[str, int] = {
    'netherlands': 1489669811637190766,
    'scotland':    1489669807770046494,
    'ukraine':     1489669357922684928,
    'wales':       1489669363781865564,
    'turkiye':     1489669362532225024,
    'switzerland': 1489669360095334540,
    'sweden':      1489669355670343771,
    'spain':       1489668719998140608,
    'slovenia':    1489668730886688918,
    'serbia':      1489668727577378918,
    'romania':     1489668726109241425,
    'portugal':    1489668723026432092,
    'poland':      1489668717473300480,
    'france':      1489662530711195658,
    'norway':      1489667362193018980,
    'hungary':     1489667359135502446,
    'italy':       1489667332182642748,
    'germany':     1489667317544779989,
    'england':     1489667365846126703,
    'denmark':     1489667364038639777,
    'albania':     1489664332240257196,
    'austria':     1489666121186414642,
    'belgium':     1489666392658546708,
    'croatia':     1489666882637402222,
}

TEAM_FLAGS: dict[str, str] = {
    'netherlands': '🇳🇱', 'scotland': '🏴󠁧󠁢󠁳󠁣󠁴󠁿', 'ukraine': '🇺🇦',
    'wales':       '🏴󠁧󠁢󠁷󠁬󠁳󠁿', 'turkiye': '🇹🇷',    'switzerland': '🇨🇭',
    'sweden':      '🇸🇪', 'spain': '🇪🇸',          'slovenia': '🇸🇮',
    'serbia':      '🇷🇸', 'romania': '🇷🇴',         'portugal': '🇵🇹',
    'poland':      '🇵🇱', 'france': '🇫🇷',           'norway': '🇳🇴',
    'hungary':     '🇭🇺', 'italy': '🇮🇹',            'germany': '🇩🇪',
    'england':     '🏴󠁧󠁢󠁥󠁮󠁧󠁿', 'denmark': '🇩🇰',    'albania': '🇦🇱',
    'austria':     '🇦🇹', 'belgium': '🇧🇪',          'croatia': '🇭🇷',
}

TEAM_CHOICES = [
    app_commands.Choice(name=k.title(), value=k)
    for k in sorted(TEAM_ROLES.keys())
]

#  staff role IDs 
MANAGER_ROLE_ID  = 1476677221245784207   # team owner / manager
ASST_ROLE_ID     = 1476677267856818236   # assistant team owner

#  free-agency channel 
FREE_AGENT_CHANNEL_ID = 1292595174232424518
FREE_AGENT_COOLDOWN   = 5 * 60 * 60      # 5 hours in seconds

#  contract log channel 
CONTRACT_LOG_CHANNEL_ID = 1476037356917227782

#  friendlies channel 
FRIENDLIES_CHANNEL_ID = 1477028031317934190

#  credentials (all from environment variables) 
def _require(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(f'Missing required environment variable: {key}')
    return val

BOT_TOKEN        = _require('BOT_TOKEN')
ROBLOX_UNIVERSE  = _require('ROBLOX_UNIVERSE')
ROBLOX_GROUP_ID  = _require('ROBLOX_GROUP_ID')
ROBLOX_API_KEY   = _require('ROBLOX_API_KEY')
DISCORD_GUILD_ID = int(_require('DISCORD_GUILD_ID'))
ROVER_SECRET     = _require('ROVER_SECRET')

# Firebase credentials are assembled from individual env vars so the private key
# (which contains newlines) does not need any special escaping in Railway.
FIREBASE_CREDS = {
    "type":                        "service_account",
    "project_id":                  _require('FIREBASE_PROJECT_ID'),
    "private_key_id":              _require('FIREBASE_PRIVATE_KEY_ID'),
    # Railway stores the key with literal \n — replace them back to real newlines
    "private_key":                 _require('FIREBASE_PRIVATE_KEY').replace('\\n', '\n'),
    "client_email":                _require('FIREBASE_CLIENT_EMAIL'),
    "client_id":                   _require('FIREBASE_CLIENT_ID'),
    "auth_uri":                    "https://accounts.google.com/o/oauth2/auth",
    "token_uri":                   "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url":        _require('FIREBASE_CLIENT_CERT_URL'),
    "universe_domain":             "googleapis.com",
}

#  firebase init 
def _init_firebase():
    cred = credentials.Certificate(FIREBASE_CREDS)
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://rfa-base-default-rtdb.europe-west1.firebasedatabase.app/'
    })

_init_firebase()

def _r(path: str):
    """Generic Firebase reference helper."""
    return fdb.reference(path)

def _now() -> str:
    return datetime.utcnow().isoformat()

#  cooldown store (Firebase) for /freeagent 
def get_fa_cooldown(guild_id: int, user_id: int) -> float:
    """Returns the Unix timestamp of the last /freeagent post, or 0."""
    raw = _r(f'rfa/{guild_id}/fa_cooldowns/{user_id}').get()
    return float(raw) if raw else 0.0

def set_fa_cooldown(guild_id: int, user_id: int):
    _r(f'rfa/{guild_id}/fa_cooldowns/{user_id}').set(time.time())

#  Firebase: misc (bans, audit, etc.) 
def audit_log(guild_id: int, action: str, data: dict):
    try:
        _r(f'rfa/{guild_id}/audit_log').push({
            'action': action, 'timestamp': _now(), **data
        })
    except Exception as e:
        print(f'[audit_log] {e}')

#  role helpers 
def tfmt(team: str) -> str:
    flag = TEAM_FLAGS.get(team.lower(), '')
    return f"{flag} {team.title()}".strip()

def get_team_role(guild: discord.Guild, team: str) -> discord.Role | None:
    rid = TEAM_ROLES.get(team.lower())
    return guild.get_role(rid) if rid else None

def get_member_team(member: discord.Member) -> str | None:
    """Returns the team key the member belongs to, or None."""
    member_role_ids = {r.id for r in member.roles}
    for team, rid in TEAM_ROLES.items():
        if rid in member_role_ids:
            return team
    return None

def is_manager(member: discord.Member) -> bool:
    """True if the member has the manager or assistant role."""
    ids = {r.id for r in member.roles}
    return MANAGER_ROLE_ID in ids or ASST_ROLE_ID in ids

def is_manager_of(member: discord.Member, team: str) -> bool:
    """True if member has BOTH the manager/asst role AND the specific team role."""
    if member.guild_permissions.administrator:
        return True
    ids = {r.id for r in member.roles}
    has_staff = MANAGER_ROLE_ID in ids or ASST_ROLE_ID in ids
    has_team  = TEAM_ROLES.get(team.lower()) in ids
    return has_staff and has_team

def get_manager_team(member: discord.Member) -> str | None:
    """Returns which team this manager manages (has both staff + team role)."""
    if not is_manager(member):
        return None
    for team, rid in TEAM_ROLES.items():
        if rid in {r.id for r in member.roles}:
            return team
    return None

def get_team_roster(guild: discord.Guild, team: str) -> list[discord.Member]:
    """All members that have the team role (excluding managers from player list if needed)."""
    role = get_team_role(guild, team)
    if not role:
        return []
    return [m for m in role.members if not m.bot]

#  signing state (Firebase, lightweight) 
def signing_open(guild_id: int) -> bool:
    raw = _r(f'rfa/{guild_id}/cfg/open').get()
    return bool(raw) if raw is not None else True

def set_signing(guild_id: int, val: bool):
    _r(f'rfa/{guild_id}/cfg/open').set(int(val))

def get_max_players(guild_id: int) -> int:
    raw = _r(f'rfa/{guild_id}/cfg/maxp').get()
    return int(raw) if raw else 25

def set_max_players(guild_id: int, val: int):
    _r(f'rfa/{guild_id}/cfg/maxp').set(val)

#  embed footer helper 
def footer(guild: discord.Guild | None) -> tuple[str, str | None]:
    icon = guild.icon.url if guild and guild.icon else None
    return 'Roblox Football Association', icon

#  roblox helpers 
def roblox_headers():
    return {'x-api-key': ROBLOX_API_KEY, 'Content-Type': 'application/json'}

ROBLOX_DATASTORE = 'ModSystem'
KOHL_DS          = 'KSave'
KOHL_TITLES      = {1:'VIP',2:'Moderator',3:'Administrator',4:'Super Admin',5:'Owner',6:'Game Creator'}

async def roblox_get_user_id(username: str) -> int | None:
    async with aiohttp.ClientSession() as s:
        async with s.post('https://users.roblox.com/v1/usernames/users',
                          json={'usernames': [username], 'excludeBannedUsers': False}) as r:
            if r.status != 200: return None
            users = (await r.json()).get('data', [])
            return users[0]['id'] if users else None

async def roblox_get_user_info(user_id: int) -> dict | None:
    async with aiohttp.ClientSession() as s:
        async with s.get(f'https://users.roblox.com/v1/users/{user_id}') as r:
            return await r.json() if r.status == 200 else None

async def roblox_ban(user_id: int, reason: str, duration_days: int | None) -> tuple[bool, str]:
    payload = {'gameJoinRestriction': {'active': True, 'privateReason': reason, 'displayReason': reason}}
    if duration_days:
        payload['gameJoinRestriction']['duration'] = f'{duration_days * 86400}s'
    async with aiohttp.ClientSession() as s:
        async with s.patch(
            f'https://apis.roblox.com/cloud/v2/universes/{ROBLOX_UNIVERSE}/user-restrictions/{user_id}',
            headers=roblox_headers(), json=payload) as r:
            return (True, 'OK') if r.status in (200, 204) else (False, f'HTTP {r.status}: {await r.text()}')

async def roblox_unban(user_id: int) -> tuple[bool, str]:
    async with aiohttp.ClientSession() as s:
        async with s.patch(
            f'https://apis.roblox.com/cloud/v2/universes/{ROBLOX_UNIVERSE}/user-restrictions/{user_id}',
            headers=roblox_headers(), json={'gameJoinRestriction': {'active': False}}) as r:
            return (True, 'OK') if r.status in (200, 204) else (False, f'HTTP {r.status}: {await r.text()}')

async def roblox_get_ban(user_id: int) -> dict | None:
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://apis.roblox.com/cloud/v2/universes/{ROBLOX_UNIVERSE}/user-restrictions/{user_id}',
            headers={'x-api-key': ROBLOX_API_KEY}) as r:
            return await r.json() if r.status == 200 else None

async def roblox_get_all_bans() -> list:
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://apis.roblox.com/cloud/v2/universes/{ROBLOX_UNIVERSE}/user-restrictions',
            headers={'x-api-key': ROBLOX_API_KEY}, params={'maxPageSize': 100}) as r:
            if r.status != 200: return []
            return [e for e in (await r.json()).get('userRestrictions', [])
                    if e.get('gameJoinRestriction', {}).get('active')]

async def roblox_get_player_count() -> int | None:
    async with aiohttp.ClientSession() as s:
        async with s.get(f'https://games.roblox.com/v1/games?universeIds={ROBLOX_UNIVERSE}') as r:
            if r.status != 200: return None
            games = (await r.json()).get('data', [])
            return games[0].get('playing') if games else None

async def roblox_get_servers() -> list:
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://games.roblox.com/v1/games/{ROBLOX_UNIVERSE}/servers/Public?limit=10') as r:
            return (await r.json()).get('data', []) if r.status == 200 else []

async def roblox_announce(topic: str, message: str) -> tuple[bool, str]:
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f'https://apis.roblox.com/messaging-service/v1/universes/{ROBLOX_UNIVERSE}/topics/{topic}',
            headers=roblox_headers(), json={'message': message}) as r:
            return (True, 'OK') if r.status in (200, 204) else (False, f'HTTP {r.status}: {await r.text()}')

async def roblox_message(topic: str, payload: dict) -> tuple[bool, str]:
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f'https://apis.roblox.com/messaging-service/v1/universes/{ROBLOX_UNIVERSE}/topics/{topic}',
            headers=roblox_headers(), json={'message': json.dumps(payload)}) as r:
            return (True, 'OK') if r.status in (200, 204) else (False, f'HTTP {r.status}: {await r.text()}')

#  datastores 
async def ds_set(key: str, value, store: str = None) -> tuple[bool, str]:
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f'https://apis.roblox.com/datastores/v1/universes/{ROBLOX_UNIVERSE}/standard-datastores/datastore/entries/entry',
            headers=roblox_headers(),
            params={'datastoreName': store or ROBLOX_DATASTORE, 'entryKey': key},
            json=value) as r:
            return (True, 'OK') if r.status in (200, 201) else (False, f'HTTP {r.status}: {await r.text()}')

async def ds_delete(key: str, store: str = None) -> tuple[bool, str]:
    async with aiohttp.ClientSession() as s:
        async with s.delete(
            f'https://apis.roblox.com/datastores/v1/universes/{ROBLOX_UNIVERSE}/standard-datastores/datastore/entries/entry',
            headers={'x-api-key': ROBLOX_API_KEY},
            params={'datastoreName': store or ROBLOX_DATASTORE, 'entryKey': key}) as r:
            return (True, 'OK') if r.status in (200, 204) else (False, f'HTTP {r.status}: {await r.text()}')

async def ds_list(prefix: str = '', store: str = None) -> list:
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://apis.roblox.com/datastores/v1/universes/{ROBLOX_UNIVERSE}/standard-datastores/datastore/entries',
            headers={'x-api-key': ROBLOX_API_KEY},
            params={'datastoreName': store or ROBLOX_DATASTORE, 'prefix': prefix, 'limit': 100}) as r:
            return (await r.json()).get('keys', []) if r.status == 200 else []

async def ds_get(key: str, store: str = None) -> dict | None:
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://apis.roblox.com/datastores/v1/universes/{ROBLOX_UNIVERSE}/standard-datastores/datastore/entries/entry',
            headers={'x-api-key': ROBLOX_API_KEY},
            params={'datastoreName': store or ROBLOX_DATASTORE, 'entryKey': key}) as r:
            return await r.json() if r.status == 200 else None

#  kohl 
async def kohl_read() -> dict:
    raw = await ds_get('KSave', store=KOHL_DS)
    if not raw or not isinstance(raw, list) or len(raw) < 1:
        return {}
    admins = {}
    for part in (raw[0] or '').strip().split():
        try:
            uid, power = part.split(':')
            admins[int(uid)] = int(power)
        except:
            continue
    return admins

async def kohl_write(admins: dict) -> tuple[bool, str]:
    raw = await ds_get('KSave', store=KOHL_DS)
    if not raw or not isinstance(raw, list):
        raw = ['', '', '']
    raw[0] = ''.join(f' {uid}:{power}' for uid, power in admins.items() if power != 0)
    return await ds_set('KSave', raw, store=KOHL_DS)

async def kohl_set_power(user_id: int, power: int) -> tuple[bool, str]:
    admins = await kohl_read()
    if power == 0:
        admins.pop(user_id, None)
    else:
        admins[user_id] = power
    return await kohl_write(admins)

async def kohl_get_username(user_id: int) -> str:
    info = await roblox_get_user_info(user_id)
    return info.get('name', str(user_id)) if info else str(user_id)

#  pin helpers 
async def upload_image_to_roblox(image_bytes: bytes, filename: str, name: str) -> tuple[bool, str]:
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

async def poll_asset_operation(operation_path: str) -> tuple[bool, str]:
    async with aiohttp.ClientSession() as s:
        for _ in range(15):
            await asyncio.sleep(3)
            async with s.get(f'https://apis.roblox.com/{operation_path}',
                             headers={'x-api-key': ROBLOX_API_KEY}) as r:
                if r.status != 200:
                    continue
                data = await r.json()
                if data.get('done'):
                    asset_id = data.get('response', {}).get('assetId')
                    return (True, str(asset_id)) if asset_id else (False, f'No assetId: {data}')
    return False, 'Timed out waiting for Roblox to process the image'

async def grant_pin_to_player(roblox_user_id: int, asset_id: str) -> tuple[bool, str]:
    ds_key = f'pins_{roblox_user_id}'
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f'https://apis.roblox.com/datastores/v1/universes/{ROBLOX_UNIVERSE}/standard-datastores/datastore/entries/entry',
            headers={'x-api-key': ROBLOX_API_KEY},
            params={'datastoreName': 'PlayerPins_v1', 'entryKey': ds_key}) as r:
            current = (await r.json()) if r.status == 200 else []
            if not isinstance(current, list):
                current = []
    asset_id_int = int(asset_id)
    if asset_id_int not in current:
        current.append(asset_id_int)
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

# 
#  SIGNING SYSTEM  (role-based, no roster DB)
# 

class SignView(discord.ui.View):
    """Shown to the player in the signing channel + DM."""

    def __init__(self, cid: str, guild_id: int, player_id: int):
        super().__init__(timeout=None)
        self.cid       = cid
        self.guild_id  = guild_id
        self.player_id = player_id
        self.accept_btn.custom_id  = f'sign_a_{guild_id}_{cid}'
        self.decline_btn.custom_id = f'sign_d_{guild_id}_{cid}'

    async def interaction_check(self, it: discord.Interaction) -> bool:
        if it.user.id != self.player_id:
            await it.response.send_message('This contract is not addressed to you.', ephemeral=True)
            return False
        return True

    async def _resolve(self, it: discord.Interaction, accepted: bool):
        row = _r(f'rfa/{self.guild_id}/contracts/{self.cid}').get()
        if not row or row.get('status') != 'Pending':
            await it.response.send_message('This contract is no longer active.', ephemeral=True)
            return

        status = 'Signed' if accepted else 'Rejected'
        _r(f'rfa/{self.guild_id}/contracts/{self.cid}').update(
            {'status': status, 'responded': _now()}
        )

        guild = bot.get_guild(self.guild_id)
        col   = C['a'] if accepted else C['d']
        team  = row['team']

        if accepted and guild:
            team_role = get_team_role(guild, team)
            if team_role:
                try:
                    member = guild.get_member(row['sg_id'])
                    if member:
                        await member.add_roles(team_role, reason=f'Signed to {team}')
                except Exception as e:
                    print(f'[sign] role grant failed: {e}')

        updated = _build_contract_embed(self.cid, row, col, guild, status)
        for btn in self.children:
            btn.disabled = True
        await it.response.edit_message(embed=updated, view=self)

        verb      = 'accepted' if accepted else 'declined'
        verb_past = 'Accepted' if accepted else 'Declined'

        # DM the player
        player_dm_embed = discord.Embed(
            color=col,
            title=f'Contract {verb_past}',
            description=(
                f'You have **{verb}** the contract offer to join **{tfmt(team)}**.\n\n'
                f'Position: {row.get("pos", "—")}\n'
                f'Role: {row.get("tier", "—")}\n'
                f'Contract ID: `{self.cid}`'
            )
        )
        ft, fi = footer(guild)
        player_dm_embed.set_footer(text=ft, icon_url=fi)
        if row.get('dm_msg_id'):
            try:
                u      = await bot.fetch_user(row['sg_id'])
                dm     = await u.create_dm()
                dm_msg = await dm.fetch_message(row['dm_msg_id'])
                await dm_msg.edit(embed=player_dm_embed, view=discord.ui.View())
            except:
                pass

        # DM the manager
        manager_dm_embed = discord.Embed(
            color=col,
            title=f'Contract {verb_past}',
            description=(
                f'<@{row["sg_id"]}> has **{verb}** your contract offer for **{tfmt(team)}**.\n\n'
                f'Position: {row.get("pos", "—")}\n'
                f'Role: {row.get("tier", "—")}\n'
                f'Contract ID: `{self.cid}`'
            )
        )
        manager_dm_embed.set_footer(text=ft, icon_url=fi)
        try:
            contractor = await bot.fetch_user(row['ct_id'])
            await contractor.send(embed=manager_dm_embed)
        except:
            pass

        # Post to contract log channel
        try:
            log_ch = bot.get_channel(CONTRACT_LOG_CHANNEL_ID)
            if log_ch:
                player_name  = row.get('sg_name', str(row['sg_id']))
                manager_name = row.get('ct_name', str(row['ct_id']))
                if accepted:
                    log_text = f'__**{player_name}**__ has accepted the offer from **{manager_name}** to join **{tfmt(team)}**.'
                else:
                    log_text = f'__**{player_name}**__ has declined the offer from **{manager_name}** to join **{tfmt(team)}**.'
                await log_ch.send(log_text)
        except Exception as e:
            print(f'[contract log] {e}')

        # Update the original command channel
        try:
            orig_ch = bot.get_channel(row.get('ch_id'))
            if orig_ch and row.get('msg_id'):
                orig_msg = await orig_ch.fetch_message(row['msg_id'])
                await orig_msg.edit(
                    content=(
                        f'Contract {verb_past} — '
                        f'<@{row["sg_id"]}> has **{verb}** the offer from <@{row["ct_id"]}> '
                        f'to join **{tfmt(team)}**.'
                    ),
                    embed=updated,
                    view=self
                )
        except:
            pass

    @discord.ui.button(label='Accept', style=discord.ButtonStyle.success,
                       custom_id='sign_a_placeholder')
    async def accept_btn(self, it: discord.Interaction, _):
        await self._resolve(it, True)

    @discord.ui.button(label='Decline', style=discord.ButtonStyle.danger,
                       custom_id='sign_d_placeholder')
    async def decline_btn(self, it: discord.Interaction, _):
        await self._resolve(it, False)


def _build_contract_embed(
    cid: str, row: dict, color: int,
    guild: discord.Guild | None, status: str | None = None
) -> discord.Embed:
    status_line = {
        'Signed':    '\n\nContract Accepted',
        'Rejected':  '\n\nContract Declined',
        'Expired':   '\n\nContract Expired',
        'Cancelled': '\n\nContract Revoked',
    }.get(status, '')

    desc = (
        'This document serves as an official binding agreement between the '
        'Player and the RFA Manager. Upon acceptance, the player '
        'commits to representing their assigned nation with full dedication.'
        + status_line
    )

    e = discord.Embed(title='Contract Offer — RFA', color=color, description=desc)
    e.add_field(name='Player',      value=f'<@{row["sg_id"]}>', inline=True)
    e.add_field(name='Team',        value=tfmt(row['team']),     inline=True)
    e.add_field(name='Position',    value=row.get('pos', '—'),   inline=True)
    e.add_field(name='Role',        value=row.get('tier', '—'),  inline=True)
    e.add_field(name='Contract ID', value=f'`{cid}`',            inline=True)
    if row.get('notes'):
        e.add_field(name='Notes', value=row['notes'], inline=False)

    team_role = get_team_role(guild, row['team']) if guild else None
    if team_role:
        e.add_field(name='Discord Role', value=team_role.mention, inline=False)

    ft, fi = footer(guild)
    e.set_footer(text=ft, icon_url=fi)
    return e


#  contract expiry loop 
@tasks.loop(seconds=30)
async def expire_loop():
    cutoff  = (datetime.utcnow() - timedelta(minutes=5)).isoformat()
    all_rfa = _r('rfa').get() or {}
    for gid_str, gdata in all_rfa.items():
        gid = int(gid_str)
        for cid, row in (gdata.get('contracts') or {}).items():
            if row.get('status') != 'Pending':
                continue
            if row.get('created', '') >= cutoff:
                continue
            _r(f'rfa/{gid}/contracts/{cid}').update(
                {'status': 'Expired', 'responded': _now()}
            )
            guild = bot.get_guild(gid)
            e     = _build_contract_embed(cid, row, C['c'], guild, 'Expired')
            blank = discord.ui.View()
            team  = row.get('team', '')

            # Edit channel message
            if guild and row.get('ch_id') and row.get('msg_id'):
                ch = guild.get_channel(row['ch_id'])
                if ch:
                    try:
                        msg = await ch.fetch_message(row['msg_id'])
                        await msg.edit(
                            content=(
                                f'Contract Expired — the offer to <@{row["sg_id"]}> '
                                f'from <@{row["ct_id"]}> to join **{tfmt(team)}** has expired.'
                            ),
                            embed=e, view=blank
                        )
                    except:
                        pass

            # Edit DM
            if row.get('dm_msg_id'):
                try:
                    u      = await bot.fetch_user(row['sg_id'])
                    dm     = await u.create_dm()
                    dm_msg = await dm.fetch_message(row['dm_msg_id'])
                    expired_embed = discord.Embed(
                        color=C['c'],
                        title='Contract Expired',
                        description=(
                            f'Your contract offer to join **{tfmt(team)}** has expired.\n\n'
                            f'Position: {row.get("pos", "—")}\n'
                            f'Role: {row.get("tier", "—")}\n'
                            f'Contract ID: `{cid}`'
                        )
                    )
                    ft, fi = footer(guild)
                    expired_embed.set_footer(text=ft, icon_url=fi)
                    await dm_msg.edit(embed=expired_embed, view=blank)
                except:
                    pass

            # DM the manager
            try:
                contractor = await bot.fetch_user(row['ct_id'])
                mgr_embed  = discord.Embed(
                    color=C['c'],
                    title='Contract Expired',
                    description=(
                        f'The contract offer to <@{row["sg_id"]}> for **{tfmt(team)}** has expired.\n\n'
                        f'Position: {row.get("pos", "—")}\n'
                        f'Role: {row.get("tier", "—")}\n'
                        f'Contract ID: `{cid}`'
                    )
                )
                ft, fi = footer(guild)
                mgr_embed.set_footer(text=ft, icon_url=fi)
                await contractor.send(embed=mgr_embed)
            except:
                pass

            # Log
            try:
                log_ch = bot.get_channel(CONTRACT_LOG_CHANNEL_ID)
                if log_ch:
                    player_name  = row.get('sg_name', str(row['sg_id']))
                    manager_name = row.get('ct_name', str(row['ct_id']))
                    await log_ch.send(
                        f'__**{player_name}**__ did not respond to the offer from **{manager_name}** '
                        f'to join **{tfmt(team)}**. The contract has expired.'
                    )
            except Exception as ex:
                print(f'[contract log expire] {ex}')

# 
#  TICKET SYSTEM
# 

class CloseTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Close Ticket', style=discord.ButtonStyle.danger,
                       emoji='', custom_id='close_ticket_btn')
    async def close(self, it: discord.Interaction, _):
        tk = _r(f'rfa/{it.guild_id}/tickets/{it.channel_id}').get()
        if not tk:
            await it.response.send_message('Not a ticket channel.', ephemeral=True)
            return
        if not (it.user.guild_permissions.manage_channels or it.user.id == tk['uid']):
            await it.response.send_message('No permission.', ephemeral=True)
            return
        await it.response.defer()
        # Build transcript
        lines = []
        async for m in it.channel.history(limit=500, oldest_first=True):
            lines.append(
                f'[{m.created_at.strftime("%Y-%m-%d %H:%M:%S")}] '
                f'{m.author.display_name}: {m.content or "[embed/attachment]"}'
            )
        log_ch_id = _r(f'rfa/{it.guild_id}/cfg/tlog').get()
        if log_ch_id:
            lch = it.guild.get_channel(int(log_ch_id))
            if lch:
                le = discord.Embed(
                    color=C['c'],
                    description=f'Ticket closed by {it.user.mention}'
                )
                le.add_field(name='Opened by', value=f'<@{tk["uid"]}>', inline=True)
                le.add_field(name='Channel',   value=it.channel.name,   inline=True)
                le.add_field(name='Opened',    value=tk['created'][:10], inline=True)
                ft, fi = footer(it.guild)
                le.set_footer(text=ft, icon_url=fi)
                await lch.send(
                    embed=le,
                    file=discord.File(
                        io.BytesIO('\n'.join(lines).encode()),
                        filename=f'transcript-{it.channel.name}.txt'
                    )
                )
        _r(f'rfa/{it.guild_id}/tickets/{it.channel_id}').update(
            {'status': 'closed', 'closed': _now()}
        )
        await it.followup.send('Closing in 3 seconds…')
        await asyncio.sleep(3)
        await it.channel.delete()


class TicketPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label='Open Ticket', style=discord.ButtonStyle.primary,
                       emoji='', custom_id='open_ticket_btn')
    async def open_ticket(self, it: discord.Interaction, _):
        tcat = _r(f'rfa/{it.guild_id}/cfg/tcat').get()
        if not tcat:
            await it.response.send_message('Ticket system not configured.', ephemeral=True)
            return
        # One open ticket per user
        tickets = _r(f'rfa/{it.guild_id}/tickets').get() or {}
        for ch_id, tk in tickets.items():
            if tk.get('uid') == it.user.id and tk.get('status') == 'open':
                ch = it.guild.get_channel(int(ch_id))
                if ch:
                    await it.response.send_message(
                        f'You already have an open ticket: {ch.mention}', ephemeral=True
                    )
                    return
        cat = it.guild.get_channel(int(tcat))
        if not cat:
            await it.response.send_message('Ticket category not found.', ephemeral=True)
            return
        mgr_rid  = _r(f'rfa/{it.guild_id}/cfg/mgr_role').get()
        amgr_rid = _r(f'rfa/{it.guild_id}/cfg/amgr_role').get()
        ow = {
            it.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            it.user:               discord.PermissionOverwrite(view_channel=True, send_messages=True, attach_files=True),
            it.guild.me:           discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True),
        }
        for rid in [mgr_rid, amgr_rid]:
            if rid:
                ro = it.guild.get_role(int(rid))
                if ro:
                    ow[ro] = discord.PermissionOverwrite(view_channel=True, send_messages=True)
        ch = await it.guild.create_text_channel(
            f'ticket-{it.user.name}', category=cat, overwrites=ow
        )
        _r(f'rfa/{it.guild_id}/tickets/{ch.id}').set(
            {'uid': it.user.id, 'status': 'open', 'created': _now(), 'closed': None}
        )
        e = discord.Embed(
            color=C['pr'],
            description=(
                f'Welcome {it.user.mention}! '
                'Please describe your issue and a staff member will assist you shortly.'
            )
        )
        ft, fi = footer(it.guild)
        e.set_footer(text=ft, icon_url=fi)
        await ch.send(embed=e, view=CloseTicketView())
        await it.response.send_message(
            f'Your ticket has been created: {ch.mention}', ephemeral=True
        )

# 
#  COMMANDS
# 

#  /contract 
@bot.tree.command(name='contract', description='Send a contract offer to a player')
@app_commands.describe(
    player='The player to offer a contract to',
    pos='Position (e.g. GK, CB, ST)',
    tier='Role (e.g. Starter, Sub, Backup)',
    notes='Optional notes',
)
@app_commands.default_permissions(manage_messages=True)
async def sign_cmd(
    it: discord.Interaction,
    player: discord.Member,
    pos: str,
    tier: str,
    notes: str = None,
):
    #  guard: only managers / assistants 
    if not is_manager(it.user) and not it.user.guild_permissions.administrator:
        await it.response.send_message(
            'You must have the **Manager** or **Assistant Manager** role to sign players.',
            ephemeral=True
        )
        return

    #  auto-detect manager's team 
    team = get_manager_team(it.user)
    if team is None and not it.user.guild_permissions.administrator:
        await it.response.send_message(
            'You must also have your **team role** assigned to sign players. '
            'Contact an administrator.',
            ephemeral=True
        )
        return
    if team is None:
        # Admin with no team role — ask them to use /forceadd instead
        await it.response.send_message(
            'Administrators without a team role should use `/forceadd` instead.',
            ephemeral=True
        )
        return

    #  basic sanity 
    if player.id == it.user.id:
        await it.response.send_message('You cannot sign yourself.', ephemeral=True)
        return
    if player.bot:
        await it.response.send_message('Bots cannot be signed.', ephemeral=True)
        return

    #  signing window 
    if not signing_open(it.guild_id):
        await it.response.send_message('The signing window is currently **closed**.', ephemeral=True)
        return

    #  already on a team? 
    existing = get_member_team(player)
    if existing:
        await it.response.send_message(
            f'{player.mention} is already signed to **{tfmt(existing)}**.', ephemeral=True
        )
        return

    #  squad cap 
    roster = get_team_roster(it.guild, team)
    if len(roster) >= get_max_players(it.guild_id):
        await it.response.send_message(
            f'**{tfmt(team)}** squad is full ({get_max_players(it.guild_id)} max).', ephemeral=True
        )
        return

    #  pending contract guard 
    contracts = _r(f'rfa/{it.guild_id}/contracts').get() or {}
    for cdata in contracts.values():
        if cdata.get('sg_id') == player.id and cdata.get('status') == 'Pending':
            await it.response.send_message(
                f'{player.mention} already has a **pending** contract offer.', ephemeral=True
            )
            return

    #  create contract 
    cid = str(random.randint(10**15, 10**16 - 1))
    row = {
        'ct_id': it.user.id, 'ct_name': it.user.name,
        'sg_id': player.id,  'sg_name': player.name,
        'team': team, 'pos': pos, 'tier': tier,
        'notes': notes, 'status': 'Pending',
        'created': _now(), 'responded': None,
        'msg_id': None, 'ch_id': it.channel_id, 'dm_msg_id': None,
    }
    e   = _build_contract_embed(cid, row, 0x2b2d31, it.guild)
    v   = SignView(cid, it.guild_id, player.id)

    await it.response.send_message(
        content=(
            f'Contract offer sent to {player.mention} by {it.user.mention} '
            '| Expires in 5 minutes'
        ),
        embed=e, view=v
    )
    pub_msg = await it.original_response()
    row['msg_id'] = pub_msg.id

    # DM the player
    dm_msg_id = None
    try:
        dm_msg = await player.send(
            content=(
                f'You have a contract offer from **{it.guild.name}** '
                f'to join **{tfmt(team)}**.\n'
                f'Head to the server to accept or decline.'
            ),
            embed=e,
            view=SignView(cid, it.guild_id, player.id),
        )
        dm_msg_id = dm_msg.id
    except:
        pass

    row['dm_msg_id'] = dm_msg_id
    _r(f'rfa/{it.guild_id}/contracts/{cid}').set(row)
    bot.add_view(v)


#  /release 
@bot.tree.command(name='release', description='Release a player from your squad')
@app_commands.describe(player='The player to release')
@app_commands.default_permissions(manage_messages=True)
async def release_cmd(it: discord.Interaction, player: discord.Member):
    if not is_manager(it.user) and not it.user.guild_permissions.administrator:
        await it.response.send_message(
            'Only managers / assistants can release players.', ephemeral=True
        )
        return

    mgr_team = get_manager_team(it.user)
    player_team = get_member_team(player)

    if not player_team:
        await it.response.send_message(
            f'{player.mention} is not on any team.', ephemeral=True
        )
        return

    # Non-admin managers may only release from their own team
    if not it.user.guild_permissions.administrator:
        if mgr_team is None or mgr_team != player_team:
            await it.response.send_message(
                f'{player.mention} is not on **your** team.', ephemeral=True
            )
            return

    team_role = get_team_role(it.guild, player_team)
    if team_role:
        try:
            await player.remove_roles(team_role, reason=f'Released from {player_team}')
        except Exception as ex:
            await it.response.send_message(f'Failed to remove role: {ex}', ephemeral=True)
            return

    e = discord.Embed(
        color=C['d'],
        description=f'{player.mention} has been released from **{tfmt(player_team)}**.'
    )
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.response.send_message(embed=e)

    try:
        await player.send(embed=discord.Embed(
            color=C['d'],
            description=f'You were released from **{tfmt(player_team)}** by {it.user.mention}.'
        ))
    except:
        pass


#  /forceadd  (admin override) 
@bot.tree.command(name='forceadd', description='[Admin] Force-add a player to a team')
@app_commands.describe(player='Player to add', team='Target team')
@app_commands.choices(team=TEAM_CHOICES)
@app_commands.default_permissions(administrator=True)
async def forceadd_cmd(it: discord.Interaction, player: discord.Member, team: str):
    existing = get_member_team(player)
    if existing:
        await it.response.send_message(
            f'{player.mention} is already on **{tfmt(existing)}**.', ephemeral=True
        )
        return
    role = get_team_role(it.guild, team)
    if not role:
        await it.response.send_message('Team role not found in this server.', ephemeral=True)
        return
    try:
        await player.add_roles(role, reason=f'Force-added to {team} by {it.user}')
    except Exception as ex:
        await it.response.send_message(f'Failed to add role: {ex}', ephemeral=True)
        return
    e = discord.Embed(
        color=C['a'],
        description=f'{player.mention} force-added to **{tfmt(team)}** by {it.user.mention}.'
    )
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.response.send_message(embed=e)


#  /teamsheet 
@bot.tree.command(name='teamsheet', description="View a nation's current squad")
@app_commands.choices(team=TEAM_CHOICES)
async def teamsheet_cmd(it: discord.Interaction, team: str):
    await it.response.defer()
    roster = get_team_roster(it.guild, team)
    e = discord.Embed(color=0x2b2d31, title=f"{tfmt(team)} — Squad Sheet")

    if not roster:
        e.description = 'No players are signed to this nation yet.'
    else:
        mgrs    = [m for m in roster if is_manager(m)]
        players = [m for m in roster if not is_manager(m)]
        lines   = []
        if mgrs:
            lines.append('**— Staff —**')
            for m in mgrs:
                tag = 'Manager' if MANAGER_ROLE_ID in {r.id for r in m.roles} else 'Assistant'
                lines.append(f'{tag} — {m.mention}')
            lines.append('')
        if players:
            lines.append('**— Players —**')
            for m in players:
                lines.append(f'{m.mention}')
        e.description = '\n'.join(lines)
        ft, fi = footer(it.guild)
        e.set_footer(text=f'RFA • {len(players)} player(s)', icon_url=fi)

    await it.followup.send(embed=e)


#  /freeagent 
@bot.tree.command(name='freeagent', description='Post your free-agent ad in the free-agency channel')
@app_commands.describe(
    position='Your position (e.g. GK, CB, ST)',
    experience='Your experience level or background (e.g. Semi-pro, 2 seasons in RFA)',
    about='Short description about yourself (optional)',
)
async def freeagent_cmd(it: discord.Interaction, position: str, experience: str, about: str = None):
    if get_member_team(it.user):
        await it.response.send_message(
            'You are already signed to a team. Release yourself first.', ephemeral=True
        )
        return

    last      = get_fa_cooldown(it.guild_id, it.user.id)
    remaining = FREE_AGENT_COOLDOWN - (time.time() - last)
    if remaining > 0:
        hrs  = int(remaining // 3600)
        mins = int((remaining % 3600) // 60)
        await it.response.send_message(
            f'You can post again in {hrs}h {mins}m.', ephemeral=True
        )
        return

    ch = it.guild.get_channel(FREE_AGENT_CHANNEL_ID)
    if not ch:
        await it.response.send_message('Free-agency channel not found.', ephemeral=True)
        return

    set_fa_cooldown(it.guild_id, it.user.id)

    e = discord.Embed(color=C['gold'], title='Free Agent Available')
    e.set_author(name=it.user.display_name, icon_url=it.user.display_avatar.url)
    e.add_field(name='Player',     value=it.user.mention,    inline=True)
    e.add_field(name='Position',   value=position.upper(),   inline=True)
    e.add_field(name='Experience', value=experience,         inline=False)
    if about:
        e.add_field(name='About', value=about, inline=False)
    e.add_field(
        name='Interested?',
        value='DM this player or have your manager contact them directly.',
        inline=False
    )
    ft, fi = footer(it.guild)
    e.set_footer(text=f'Roblox Football Association • {datetime.utcnow().strftime("%d/%m/%Y %H:%M")} UTC', icon_url=fi)

    await ch.send(content=it.user.mention, embed=e)
    await it.response.send_message(
        f'Your free-agent post has been sent to {ch.mention}!', ephemeral=True
    )


#  /friendly 
@bot.tree.command(name='friendly', description='Request a friendly match — pings team owners and assistants')
async def friendlies_cmd(it: discord.Interaction):
    if not is_manager(it.user) and not it.user.guild_permissions.administrator:
        await it.response.send_message(
            'Only Managers and Assistant Managers can request friendlies.',
            ephemeral=True
        )
        return

    my_team = get_manager_team(it.user)
    if my_team is None and not it.user.guild_permissions.administrator:
        await it.response.send_message(
            'You do not have a team role assigned. Contact an administrator.',
            ephemeral=True
        )
        return
    if my_team is None:
        await it.response.send_message(
            'Administrators must have a team role to request friendlies.',
            ephemeral=True
        )
        return

    ch = bot.get_channel(FRIENDLIES_CHANNEL_ID)
    if not ch:
        await it.response.send_message('Friendlies channel not found.', ephemeral=True)
        return

    mgr_mention  = f'<@&{MANAGER_ROLE_ID}>'
    asst_mention = f'<@&{ASST_ROLE_ID}>'

    e = discord.Embed(
        color=C['pr'],
        title='Friendly Match Request',
        description=(
            f'**{tfmt(my_team)}** is looking for a friendly.\n'
            f'Contact {it.user.mention} to arrange a match.'
        )
    )
    e.add_field(name='Requested by', value=it.user.mention, inline=True)
    e.add_field(name='Nation',       value=tfmt(my_team),   inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=f'Roblox Football Association • {datetime.utcnow().strftime("%d/%m/%Y %H:%M")} UTC', icon_url=fi)

    await ch.send(content=f'{mgr_mention} {asst_mention}', embed=e)
    await it.response.send_message('Friendly request posted!', ephemeral=True)


#  /signing 
@bot.tree.command(name='signing', description='Toggle the signing window open or closed')
@app_commands.choices(status=[
    app_commands.Choice(name='Open',   value=1),
    app_commands.Choice(name='Closed', value=0),
])
@app_commands.default_permissions(administrator=True)
async def signing_cmd(it: discord.Interaction, status: int):
    set_signing(it.guild_id, bool(status))
    e = discord.Embed(
        color=C['a'] if status else C['d'],
        description=f'Signing window is now **{"Open " if status else "Closed "}**.'
    )
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.response.send_message(embed=e)


#  /config 
@bot.tree.command(name='config', description='Configure bot settings')
@app_commands.default_permissions(administrator=True)
async def config_cmd(
    it: discord.Interaction,
    signing_open_flag: bool = None,
    max_players: int = None,
    ticket_category: discord.CategoryChannel = None,
    ticket_log: discord.TextChannel = None,
    manager_role: discord.Role = None,
    assistant_manager_role: discord.Role = None,
):
    updates = {}
    if signing_open_flag is not None: updates['open']      = int(signing_open_flag)
    if max_players:                   updates['maxp']      = max_players
    if ticket_category:               updates['tcat']      = ticket_category.id
    if ticket_log:                    updates['tlog']      = ticket_log.id
    if manager_role:                  updates['mgr_role']  = manager_role.id
    if assistant_manager_role:        updates['amgr_role'] = assistant_manager_role.id
    if updates:
        _r(f'rfa/{it.guild_id}/cfg').update(updates)

    cfg = _r(f'rfa/{it.guild_id}/cfg').get() or {}
    e = discord.Embed(title='Server Configuration', color=C['pr'])
    e.add_field(name='Signing',           value='Open ' if cfg.get('open', 1) else 'Closed ', inline=True)
    e.add_field(name='Max Players/Squad', value=str(cfg.get('maxp', 25)),                          inline=True)
    e.add_field(name='Ticket Category',   value=f'<#{cfg["tcat"]}>' if cfg.get('tcat') else 'Not set', inline=True)
    e.add_field(name='Ticket Log',        value=f'<#{cfg["tlog"]}>' if cfg.get('tlog') else 'Not set', inline=True)
    e.add_field(name='Manager Role',      value=f'<@&{cfg["mgr_role"]}>' if cfg.get('mgr_role') else 'Not set', inline=True)
    e.add_field(name='Asst. Mgr Role',    value=f'<@&{cfg["amgr_role"]}>' if cfg.get('amgr_role') else 'Not set', inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.response.send_message(embed=e, ephemeral=True)


#  /ticket 
@bot.tree.command(name='ticket', description='Post the ticket panel in this channel')
@app_commands.default_permissions(administrator=True)
async def ticket_panel_cmd(it: discord.Interaction):
    tcat = _r(f'rfa/{it.guild_id}/cfg/tcat').get()
    if not tcat:
        await it.response.send_message(
            'Set a ticket category first with `/config`.', ephemeral=True
        )
        return
    e = discord.Embed(
        color=C['pr'],
        description='Click below to open a support ticket. A private channel will be created for you.'
    )
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.channel.send(embed=e, view=TicketPanelView())
    await it.response.send_message('Ticket panel posted.', ephemeral=True)


#  /serverstatus 
@bot.tree.command(name='serverstatus', description='Check live player count and servers')
async def serverstatus_cmd(it: discord.Interaction):
    await it.response.defer()
    player_count = await roblox_get_player_count()
    servers      = await roblox_get_servers()
    e = discord.Embed(title='RFA Universe Status', color=C['pr'])
    e.add_field(name='Players Online', value=str(player_count) if player_count is not None else 'Unavailable', inline=True)
    e.add_field(name='Active Servers', value=str(len(servers)), inline=True)
    if servers:
        e.add_field(
            name='Server List (top 5)',
            value='\n'.join(
                f'`{sv.get("playing",0)}/{sv.get("maxPlayers","?")}` players — '
                f'ping {sv.get("ping","?")}ms'
                for sv in servers[:5]
            ),
            inline=False
        )
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)


#  /rban 
@bot.tree.command(name='rban', description='Ban a player from the Roblox game')
@app_commands.default_permissions(administrator=True)
async def rban_cmd(it: discord.Interaction, username: str, reason: str, duration_days: int = None):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.')
        return
    success, msg = await roblox_ban(user_id, reason, duration_days)
    if not success:
        await it.followup.send(f'Ban failed: `{msg}`')
        return
    dur = f'{duration_days} day(s)' if duration_days else 'Permanent'
    _r(f'rfa/{it.guild_id}/roblox_bans/{user_id}').set({
        'username': username, 'reason': reason,
        'duration_days': duration_days, 'permanent': duration_days is None,
        'banned_by': it.user.id, 'banned_at': _now()
    })
    audit_log(it.guild_id, 'rban', {
        'username': username, 'user_id': user_id,
        'reason': reason, 'duration': dur,
        'by': it.user.name, 'by_id': it.user.id
    })
    await roblox_message('ChatLog', {
        'scope': 'all', 'color': 'red',
        'text': f'[BAN] {username} banned. Reason: {reason} ({dur})',
        'sender': 'RFA System'
    })
    e = discord.Embed(color=C['d'], title='Roblox Ban Issued')
    e.add_field(name='Username',  value=username,        inline=True)
    e.add_field(name='User ID',   value=str(user_id),    inline=True)
    e.add_field(name='Duration',  value=dur,             inline=True)
    e.add_field(name='Reason',    value=reason,          inline=False)
    e.add_field(name='Banned by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)


#  /runban 
@bot.tree.command(name='runban', description='Unban a player from the Roblox game')
@app_commands.default_permissions(administrator=True)
async def runban_cmd(it: discord.Interaction, username: str):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.')
        return
    success, msg = await roblox_unban(user_id)
    if not success:
        await it.followup.send(f'Unban failed: `{msg}`')
        return
    _r(f'rfa/{it.guild_id}/roblox_bans/{user_id}').delete()
    audit_log(it.guild_id, 'runban', {
        'username': username, 'user_id': user_id,
        'by': it.user.name, 'by_id': it.user.id
    })
    await roblox_message('ChatLog', {
        'scope': 'all', 'color': 'green',
        'text': f'[UNBAN] {username} unbanned.', 'sender': 'RFA System'
    })
    e = discord.Embed(color=C['a'], title='Roblox Ban Removed')
    e.add_field(name='Username',    value=username,        inline=True)
    e.add_field(name='Unbanned by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)


#  /rbaninfo 
@bot.tree.command(name='rbaninfo', description='Check ban status of a Roblox player')
@app_commands.default_permissions(administrator=True)
async def rbaninfo_cmd(it: discord.Interaction, username: str):
    await it.response.defer(ephemeral=True)
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.', ephemeral=True)
        return
    data        = await roblox_get_ban(user_id)
    restriction = data.get('gameJoinRestriction', {}) if data else {}
    if not restriction.get('active', False):
        e = discord.Embed(color=C['a'], title='Not Banned',
                          description=f'**{username}** has no active ban.')
        e.add_field(name='User ID', value=str(user_id), inline=True)
        await it.followup.send(embed=e, ephemeral=True)
        return
    duration    = restriction.get('duration')
    dur_str     = f"{int(duration.rstrip('s')) // 86400} day(s)" if duration else 'Permanent'
    fb_data     = _r(f'rfa/{it.guild_id}/roblox_bans/{user_id}').get() or {}
    e = discord.Embed(color=C['d'], title='Player is Banned')
    e.add_field(name='Username',       value=username,                                              inline=True)
    e.add_field(name='User ID',        value=str(user_id),                                          inline=True)
    e.add_field(name='Duration',       value=dur_str,                                               inline=True)
    e.add_field(name='Reason',         value=restriction.get('displayReason', 'No reason'),         inline=False)
    e.add_field(name='Banned by',      value=f'<@{fb_data["banned_by"]}>' if fb_data.get('banned_by') else 'Unknown', inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e, ephemeral=True)


#  /rbans 
@bot.tree.command(name='rbans', description='List all currently banned Roblox players')
@app_commands.default_permissions(administrator=True)
async def rbans_cmd(it: discord.Interaction):
    await it.response.defer(ephemeral=True)
    bans = await roblox_get_all_bans()
    if not bans:
        await it.followup.send('No active bans found.', ephemeral=True)
        return
    e = discord.Embed(title=f'Active Bans ({len(bans)})', color=C['d'])
    lines = []
    for ban in bans[:20]:
        uid         = ban.get('user', '').split('/')[-1]
        restriction = ban.get('gameJoinRestriction', {})
        duration    = restriction.get('duration')
        dur_str     = f"{int(duration.rstrip('s')) // 86400}d" if duration else 'Perm'
        fb          = _r(f'rfa/{it.guild_id}/roblox_bans/{uid}').get() or {}
        name        = fb.get('username', f'ID:{uid}')
        reason      = restriction.get('displayReason', 'No reason')[:40]
        lines.append(f'`{name}` — {dur_str} — {reason}')
    e.description = '\n'.join(lines)
    ft, fi = footer(it.guild)
    e.set_footer(text=f'Showing {min(len(bans),20)} of {len(bans)} | RFA', icon_url=fi)
    await it.followup.send(embed=e, ephemeral=True)


#  /announce 
ANNOUNCE_COLORS = [
    app_commands.Choice(name=n, value=n.lower())
    for n in ['White','Red','Green','Blue','Yellow','Orange','Purple','Cyan','Pink']
]
DISCORD_COLOR_MAP = {
    'white':0xffffff,'red':0xed4245,'green':0x57f287,'blue':0x5865f2,
    'yellow':0xfee75c,'orange':0xfaa61a,'purple':0x9b59b6,'cyan':0x1abc9c,'pink':0xff69b4
}

@bot.tree.command(name='announce', description='Broadcast a message into the Roblox game')
@app_commands.choices(color=ANNOUNCE_COLORS)
@app_commands.default_permissions(administrator=True)
async def announce_cmd(it: discord.Interaction, message: str, color: str = 'white', topic: str = 'Announcements'):
    await it.response.defer()
    success, msg = await roblox_announce(
        topic, json.dumps({'text': message, 'sender': it.user.name, 'color': color})
    )
    if not success:
        await it.followup.send(f'Announce failed: `{msg}`')
        return
    audit_log(it.guild_id, 'announce', {
        'message': message, 'color': color, 'topic': topic,
        'by': it.user.name, 'by_id': it.user.id
    })
    e = discord.Embed(color=DISCORD_COLOR_MAP.get(color, 0xffffff), title='Announcement Sent')
    e.add_field(name='Message', value=message,         inline=False)
    e.add_field(name='Color',   value=color.title(),   inline=True)
    e.add_field(name='Topic',   value=f'`{topic}`',    inline=True)
    e.add_field(name='Sent by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)


#  /mod 
@bot.tree.command(name='mod', description='Give a player mod in a specific Roblox server')
@app_commands.default_permissions(administrator=True)
async def mod_cmd(it: discord.Interaction, server: str, username: str):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.')
        return
    await ds_set(f'mod_{user_id}', {
        'type': 'server', 'server': server, 'username': username,
        'userId': user_id, 'granted_by': it.user.name, 'granted_at': _now()
    })
    await kohl_set_power(user_id, 2)
    await roblox_message('ModSystem', {
        'action': 'grant', 'userId': user_id,
        'username': username, 'modType': 'server', 'server': server
    })
    audit_log(it.guild_id, 'mod', {
        'username': username, 'user_id': user_id,
        'server': server, 'type': 'server',
        'by': it.user.name, 'by_id': it.user.id
    })
    e = discord.Embed(color=C['a'], title='Server Mod Granted')
    e.add_field(name='Username',   value=username,        inline=True)
    e.add_field(name='User ID',    value=str(user_id),    inline=True)
    e.add_field(name='Server',     value=server,          inline=True)
    e.add_field(name='Granted by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)


#  /permmod 
@bot.tree.command(name='permmod', description='Give a player permanent mod across all servers')
@app_commands.default_permissions(administrator=True)
async def permmod_cmd(it: discord.Interaction, username: str):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.')
        return
    await ds_set(f'mod_{user_id}', {
        'type': 'permanent', 'username': username,
        'userId': user_id, 'granted_by': it.user.name, 'granted_at': _now()
    })
    await kohl_set_power(user_id, -3)
    await roblox_message('ModSystem', {
        'action': 'grant', 'userId': user_id,
        'username': username, 'modType': 'permanent'
    })
    audit_log(it.guild_id, 'permmod', {
        'username': username, 'user_id': user_id,
        'type': 'permanent', 'by': it.user.name, 'by_id': it.user.id
    })
    e = discord.Embed(color=C['pr'], title='Permanent Mod Granted')
    e.add_field(name='Username',   value=username,        inline=True)
    e.add_field(name='User ID',    value=str(user_id),    inline=True)
    e.add_field(name='Scope',      value='All Servers',   inline=True)
    e.add_field(name='Granted by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)


#  /unmod 
@bot.tree.command(name='unmod', description='Remove mod from a Roblox player')
@app_commands.default_permissions(administrator=True)
async def unmod_cmd(it: discord.Interaction, username: str):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.')
        return
    ds_ok, ds_msg = await ds_delete(f'mod_{user_id}')
    ds_note = ''
    if not ds_ok:
        if '404' in ds_msg or 'NOT_FOUND' in ds_msg:
            ds_note = 'No DataStore entry — was modded in-game via Kohl'
        else:
            await it.followup.send(f'DataStore error: `{ds_msg}`')
            return
    ms_ok, ms_msg = await roblox_message('ModSystem', {
        'action': 'revoke', 'userId': user_id, 'username': username
    })
    if not ms_ok:
        await it.followup.send(f'MessagingService failed: `{ms_msg}`')
        return
    await kohl_set_power(user_id, 0)
    audit_log(it.guild_id, 'unmod', {
        'username': username, 'user_id': user_id,
        'by': it.user.name, 'by_id': it.user.id,
        'note': ds_note or None
    })
    await roblox_message('ChatLog', {
        'scope': 'all', 'color': 'red',
        'text': f'[UNMOD] {username} mod removed.', 'sender': 'RFA System'
    })
    e = discord.Embed(color=C['d'], title='Mod Removed')
    e.add_field(name='Username',   value=username,        inline=True)
    e.add_field(name='User ID',    value=str(user_id),    inline=True)
    e.add_field(name='Removed by', value=it.user.mention, inline=True)
    if ds_note:
        e.add_field(name='Note', value=ds_note, inline=False)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)


#  /modlist 
@bot.tree.command(name='modlist', description='List all mods/admins from DataStore and Kohl')
@app_commands.default_permissions(administrator=True)
async def modlist_cmd(it: discord.Interaction):
    await it.response.defer(ephemeral=True)
    ds_keys     = await ds_list(prefix='mod_')
    kohl_admins = await kohl_read()
    unified     = {}
    for entry in ds_keys:
        data = await ds_get(entry.get('key', ''))
        if not data: continue
        uid = data.get('userId')
        if not uid: continue
        unified[uid] = {
            'username': data.get('username', str(uid)),
            'power':    2 if data.get('type') == 'server' else 3,
            'source':   'Discord',
            'detail':   f'Server: {data.get("server","?")}' if data.get('type') == 'server' else 'Permanent',
        }
    for uid, power in kohl_admins.items():
        abs_power = abs(power)
        if abs_power < 1: continue
        username = unified.get(uid, {}).get('username') or await kohl_get_username(uid)
        unified[uid] = {
            'username': username,
            'power':    abs_power,
            'source':   'Both' if uid in unified else 'Kohl',
            'detail':   'Permanent' if power < 0 else 'Temporary',
        }
    if not unified:
        await it.followup.send('No mods or admins found.', ephemeral=True)
        return
    lines = [
        f'**{d["username"]}** — Level {d["power"]} ({d["detail"]}) `[{d["source"]}]`'
        for _, d in sorted(unified.items(), key=lambda x: x[1]['power'], reverse=True)
    ]
    e = discord.Embed(title='Staff List', color=C['pr'], description='\n'.join(lines))
    e.set_footer(text=f'{len(lines)} total | Roblox Football Association')
    await it.followup.send(embed=e, ephemeral=True)


#  /setpower 
@bot.tree.command(name='setpower', description="Set a Roblox user's power level in Kohl (0-6)")
@app_commands.default_permissions(administrator=True)
async def setpower_cmd(it: discord.Interaction, username: str, power: int, permanent: bool = True):
    if not 0 <= power <= 6:
        await it.response.send_message('Power must be between 0 and 6.', ephemeral=True)
        return
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.')
        return
    ok, msg = await kohl_set_power(
        user_id, (-power if permanent else power) if power > 0 else 0
    )
    if not ok:
        await it.followup.send(f'Failed to update KSave: `{msg}`')
        return
    await roblox_message('ModSystem', {
        'action': 'grant' if power > 0 else 'revoke',
        'userId': user_id, 'username': username, 'power': power
    })
    audit_log(it.guild_id, 'setpower', {
        'username': username, 'user_id': user_id,
        'power': power, 'permanent': permanent,
        'by': it.user.name, 'by_id': it.user.id
    })
    e = discord.Embed(
        color=C['d'] if power == 0 else C['a'],
        title='Power Removed' if power == 0 else 'Power Set'
    )
    e.add_field(name='Username', value=username, inline=True)
    e.add_field(name='User ID',  value=str(user_id), inline=True)
    e.add_field(name='Power',    value=f'{power} — {KOHL_TITLES.get(power, "Removed")}', inline=True)
    if power > 0:
        e.add_field(name='Type', value='Permanent' if permanent else 'Temporary', inline=True)
    e.add_field(name='Set by', value=it.user.mention, inline=True)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)


#  /whois 
@bot.tree.command(name='whois', description='Look up a Roblox user and their moderation history')
@app_commands.default_permissions(administrator=True)
async def whois_cmd(it: discord.Interaction, username: str):
    await it.response.defer()
    user_id = await roblox_get_user_id(username)
    if not user_id:
        await it.followup.send(f'Roblox user **{username}** not found.')
        return
    info, ban_data, mod_ds = (
        await roblox_get_user_info(user_id),
        await roblox_get_ban(user_id),
        await ds_get(f'mod_{user_id}')
    )
    restriction = ban_data.get('gameJoinRestriction', {}) if ban_data else {}
    ban_str = 'None'
    if restriction.get('active', False):
        duration = restriction.get('duration')
        ban_str = (
            f'{"Permanent" if not duration else f"{int(duration.rstrip(chr(115))) // 86400}d"}'
            f' — {restriction.get("displayReason","No reason")}'
        )
    mod_str = 'None'
    if mod_ds:
        if mod_ds.get('type') == 'permanent':
            mod_str = f'Permanent (by {mod_ds.get("granted_by","?")} on {mod_ds.get("granted_at","?")[:10]})'
        else:
            mod_str = f'Server: `{mod_ds.get("server","?")}` (by {mod_ds.get("granted_by","?")})'

    # Audit log entries for this user
    all_logs = _r(f'rfa/{it.guild_id}/audit_log').get() or {}
    history  = []
    for entry in all_logs.values():
        if (str(entry.get('username', '')).lower() == username.lower()
                or str(entry.get('user_id', '')) == str(user_id)):
            ts     = entry.get('timestamp', '')[:16].replace('T', ' ')
            detail = entry.get('reason') or entry.get('server') or entry.get('type') or ''
            history.append(
                f'`{ts}` **{entry.get("action","?").upper()}** by {entry.get("by","?")}'
                + (f' — {detail}' if detail else '')
            )

    e = discord.Embed(color=C['c'], title=f'Whois: {username}')
    e.add_field(name='Display Name',  value=info.get('displayName', username) if info else username, inline=True)
    e.add_field(name='User ID',       value=str(user_id),                                            inline=True)
    e.add_field(name='Joined Roblox', value=info.get('created', '')[:10] if info else 'Unknown',    inline=True)
    e.add_field(name='Universe Ban',  value=ban_str,                                                 inline=False)
    e.add_field(name='Mod Status',    value=mod_str,                                                 inline=False)
    if info and info.get('description'):
        e.add_field(name='Bio', value=info['description'][:200], inline=False)
    e.add_field(
        name=f'Log History ({len(history)} entries)',
        value='\n'.join(history[-10:]) if history else 'No entries found.',
        inline=False
    )
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.followup.send(embed=e)


#  /logs 
@bot.tree.command(name='logs', description='View the moderation audit log')
@app_commands.default_permissions(administrator=True)
async def logs_cmd(it: discord.Interaction, action: str = None, limit: int = 20):
    await it.response.defer(ephemeral=True)
    limit    = min(max(limit, 1), 50)
    all_logs = _r(f'rfa/{it.guild_id}/audit_log').get() or {}
    entries  = sorted(all_logs.values(), key=lambda x: x.get('timestamp', ''), reverse=True)
    if action:
        entries = [e for e in entries if e.get('action', '').lower() == action.lower()]
    entries = entries[:limit]
    if not entries:
        await it.followup.send('No log entries found.', ephemeral=True)
        return
    lines = []
    for entry in entries:
        ts     = entry.get('timestamp', '')[:16].replace('T', ' ')
        target = entry.get('username') or entry.get('message', '')
        detail = (entry.get('reason') or entry.get('server') or
                  entry.get('type') or entry.get('color') or '')
        line   = f'`{ts}` **{entry.get("action","?").upper()}**'
        if target: line += f' — `{target}`'
        if entry.get('by'): line += f' by {entry["by"]}'
        if detail: line += f' — {detail}'
        lines.append(line)
    e = discord.Embed(
        title='Audit Log' + (f' — {action.upper()}' if action else ''),
        color=C['c'], description='\n'.join(lines)
    )
    ft, fi = footer(it.guild)
    e.set_footer(text=f'Showing {len(entries)} entries | RFA', icon_url=fi)
    await it.followup.send(embed=e, ephemeral=True)


#  /addpin 
@bot.tree.command(name='addpin', description='Upload an image as a pin and grant it to a Roblox player')
@app_commands.default_permissions(administrator=True)
async def addpin_cmd(it: discord.Interaction, roblox_username: str, image: discord.Attachment):
    if not image.content_type or not image.content_type.startswith('image/'):
        await it.response.send_message('Please attach a valid image (PNG, JPG, etc.)', ephemeral=True)
        return
    await it.response.defer()
    roblox_user_id = await roblox_get_user_id(roblox_username)
    if not roblox_user_id:
        await it.followup.send(f'Roblox user **{roblox_username}** not found.')
        return
    async with aiohttp.ClientSession() as s:
        async with s.get(image.url) as r:
            if r.status != 200:
                await it.followup.send('Failed to download the image from Discord.')
                return
            image_bytes = await r.read()
    await it.followup.send(f'Uploading pin for **{roblox_username}**… this may take ~30 seconds.')
    success, result = await upload_image_to_roblox(image_bytes, image.filename, f'{roblox_username} Pin')
    if not success:
        await it.edit_original_response(content=f'Upload failed: {result}')
        return
    ok, asset_id = await poll_asset_operation(result)
    if not ok:
        await it.edit_original_response(content=f'{asset_id}')
        return
    granted, grant_msg = await grant_pin_to_player(roblox_user_id, asset_id)
    if not granted:
        await it.edit_original_response(
            content=f' Image uploaded (ID: `{asset_id}`) but DataStore grant failed: {grant_msg}'
        )
        return
    audit_log(it.guild_id, 'addpin', {
        'roblox_username': roblox_username, 'roblox_user_id': roblox_user_id,
        'asset_id': asset_id, 'by': it.user.name, 'by_id': it.user.id
    })
    e = discord.Embed(color=C['a'], title='Pin Granted')
    e.add_field(name='Player',     value=roblox_username,              inline=True)
    e.add_field(name='Roblox ID',  value=str(roblox_user_id),          inline=True)
    e.add_field(name='Asset ID',   value=f'`{asset_id}`',              inline=True)
    e.add_field(name='rbxassetid', value=f'`rbxassetid://{asset_id}`', inline=False)
    e.add_field(name='Granted by', value=it.user.mention,              inline=True)
    e.set_thumbnail(url=image.url)
    ft, fi = footer(it.guild)
    e.set_footer(text=ft, icon_url=fi)
    await it.edit_original_response(content=None, embed=e)


# 
#  ROVER WEB SERVER
# 
async def handle_check_member(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except:
        return web.json_response({'verified': False}, status=400)
    if data.get('secret') != ROVER_SECRET:
        return web.json_response({'verified': False}, status=403)
    username = data.get('username', '').lower().strip()
    if not username:
        return web.json_response({'verified': False})
    guild = bot.get_guild(DISCORD_GUILD_ID)
    if not guild:
        return web.json_response({'verified': False})
    for member in guild.members:
        if member.nick and member.nick.lower() == username:
            return web.json_response({'verified': True})
    return web.json_response({'verified': False})

async def start_web_server():
    app = web.Application()
    app.router.add_post('/check-member', handle_check_member)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    print('Web server running on port 8080')


# 
#  BOT EVENTS
# 
@bot.event
async def on_ready():
    print(f'Online: {bot.user}')

    # Re-register persistent views for contracts still pending
    all_rfa = _r('rfa').get() or {}
    for gid_str, gdata in all_rfa.items():
        for cid, row in (gdata.get('contracts') or {}).items():
            if row.get('status') == 'Pending':
                bot.add_view(SignView(cid, int(gid_str), row.get('sg_id', 0)))

    bot.add_view(CloseTicketView())
    bot.add_view(TicketPanelView())

    if not expire_loop.is_running():
        expire_loop.start()

    await start_web_server()

    try:
        synced = await bot.tree.sync()
        print(f'Synced {len(synced)} commands')
    except Exception as ex:
        print(f'Sync error: {ex}')


if __name__ == '__main__':
    bot.run(BOT_TOKEN)
