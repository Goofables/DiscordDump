import time
from json import load
from multiprocessing import Process
from time import time

import pymysql

from utils import APIUtil, parse_time

login = load(open("db.json", 'r'))

dumped_channels = []

querys = {
    "message": """INSERT INTO messages (`id`, `channel`, `author`, `content`, `timestamp`, `type`, `attachments`, `embeds`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
    "guild": """INSERT INTO guilds (`id`,`owner`,`name`,`verification`) VALUES (%s, %s, %s, %s)""",
    "channel": """INSERT  INTO channels (`id`,`guild`,`name`,`topic`,`nsfw`,`last_message_id`,`type`) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
    "user": """INSERT IGNORE INTO users (`id`,`username`,`discriminator`, `bot`) VALUES (%s, %s, %s, %s)""",
    "guild_user": """INSERT INTO guild_users (`user`,`guild`,`joined`, `nick`) VALUES (%s, %s, %s, %s)""",
}


class Totals:
    guilds = 0
    channels = 0
    messages = 0
    new_messages = 0
    users = 0


def init_db():
    db = None
    try:
        db = pymysql.connect(login["host"], login["user"], login["pass"], login["db"])
        cursor = db.cursor()
        cursor.execute('SET NAMES utf8mb4;')
        cursor.execute('SET CHARACTER SET utf8mb4;')
        cursor.execute('SET character_set_connection=utf8mb4;')
        cursor.execute("SELECT VERSION()")
        data = cursor.fetchone()
        print(f"MySQL Version: {data}")
        cursor.execute("""create table if not exists users (
        id BIGINT(18) not null,
        username VARCHAR(32) not null,
        discriminator INT(4) not null,
        bot TINYINT(1) not NULL,
        constraint user_id
            primary key (id))""")  # Users
        cursor.execute("""create table if not exists guilds (
        id BIGINT(18) not null,
        owner BIGINT(18) null,
        name VARCHAR(100) not null,
        verification INT(1) null,
        constraint guild_id
            primary key (id))""")  # Guilds
        cursor.execute("""create table if not exists channels (
        id BIGINT(18) not null,
        guild BIGINT(18) not null,
        name VARCHAR(100) not null,
        topic VARCHAR(1024) null,
        nsfw BIT not null,
        last_message_id BIGINT(18),
        type INT(1) not null,
        constraint chan_id
            primary key (id),
        constraint ch_guild_fk
            foreign key (guild) references guilds (`id`)
                on update cascade on delete cascade)""")  # Channels
        cursor.execute("""create table if not exists messages (
        id BIGINT(18) not null,
        author BIGINT(18) null,
        channel BIGINT(18) not null,
        content TEXT(2000) not null,
        timestamp TIMESTAMP(6) not null,
        type SMALLINT(2) not null,
        attachments TEXT null,
        embeds TEXT null,
        constraint msg_id
            primary key (id),
        constraint msg_ch_fk
            foreign key (channel) references channels (`id`)
                on update cascade on delete cascade)""")  # Messages
        cursor.execute("""create table if not exists guild_users (
        user BIGINT(18) not null,
        guild BIGINT(18) not null,
        joined TIMESTAMP(6) not null,
        nick VARCHAR(32) null,
        constraint user_conn_pk
            primary key (user, guild),
        constraint guild_user_fk
            foreign key (user) references users (`id`)
                on update cascade on delete cascade,
        constraint guild_guild_fk
            foreign key (guild) references guilds (`id`)
                on update cascade on delete cascade)""")  # Guild users
        return db, cursor
    except Exception as e:
        print(e)
        print("Error! Could not login")
        if db is not None:
            db.close()
        exit()


def insert(query: str, data, db, cursor):
    if not isinstance(data, list):
        data = [data]
    if len(data) == 0:
        return
    try:
        cursor.executemany(query, data)
        db.commit()
        return True
    except pymysql.err.IntegrityError as ie:
        if ie.args[0] == 1062:
            return False
        print(f"Couldn't log data \"{data}\"")
        print(f"IntegrityError {ie}")
        exit()
    except pymysql.err.ProgrammingError as pe:
        print(f"Couldn't log data \"{data}\"")
        print(f"ProgrammingError {pe}")
        exit()
    except pymysql.err.OperationalError as oe:
        # print(f"Operational error with \"{data}\"")
        print(f"Error: {oe}")
        return False
        # print("Attempting manual insert")
        # a = []
        # for d in data:
        #     a.insert(0, d)
        # for d in a:
        #     print(query % d)
        #     cursor.execute(query, d)
        #     db.commit()
        # print("Exiting")
        # exit(0)


def dump(token, name, bot=True):
    api = APIUtil(token, bot)
    db, cursor = init_db()
    print(f"\nStarting dump for {name}")
    start_time = time()

    class New:
        guilds = 0
        channels = 0
        messages = 0
        users = 0

    guilds = api.get("/users/@me/guilds")
    for guild in guilds:
        New.guilds += 1
        print(f"Bot {name} Guild {guild['name']} ({guild['id']})")
        insert(querys['guild'], (guild['id'], None, guild['name'], None), db, cursor)

        last_user_id = 0
        while True:
            users = api.get(f"/guilds/{guild['id']}/members?limit=1000&after={last_user_id}")
            if "code" in users and users["code"] == 50001:
                break
            users_to_insert = []
            guild_users_to_insert = []
            for user in users:
                New.users += 1
                users_to_insert.append((user['user']['id'], user['user']['username'], user['user']['discriminator'],
                                        user['user'].get('bot', False)))
                guild_users_to_insert.append(
                    (user['user']['id'], guild['id'], parse_time(user['joined_at']), user['nick']))
                if int(user['user']['id']) > last_user_id:
                    last_user_id = int(user['user']['id'])
            insert(querys['user'], users_to_insert, db, cursor)
            insert(querys['guild_user'], guild_users_to_insert, db, cursor)
            if len(users) < 1000:
                break

        channels = api.get(f"/guilds/{guild['id']}/channels")
        for channel in channels:
            if channel['id'] in dumped_channels:
                continue
            New.channels += 1
            # print(f"Bot {name} Guild {guild['name']} Channel {channel['name']} ({channel['id']})")
            insert(querys['channel'], (
                channel['id'], channel['guild_id'], channel['name'], channel.get('topic', None), channel['nsfw'],
                channel.get('last_message_id', None), channel['type']), db, cursor)
            if channel['type'] in [2, 4]:  # Voice channels and categories
                dumped_channels.append(channel['id'])
                continue

            last_message_id = 0
            cursor.execute("SELECT MAX(id),count(*) FROM messages WHERE channel = %s;", channel['id'])
            if cursor.rowcount > 0:
                last_message_id, m = cursor.fetchone()
                Totals.messages += m
            if last_message_id is None:
                last_message_id = 0
            m = 0
            while True:
                messages = api.get(f"/channels/{channel['id']}/messages?limit=100&after={last_message_id}")
                if "code" in messages and messages["code"] == 50001:
                    break
                else:
                    dumped_channels.append(channel['id'])
                messages_to_insert = []
                for message in messages:
                    m += 1
                    New.messages += 1
                    # print(message)
                    # print(f"        {message['id']} {message['content']}")
                    messages_to_insert.append(
                        (message['id'], message['channel_id'], message['author']['id'], message['content'],
                         parse_time(message['timestamp']), message['type'],
                         str(message['attachments']), str(message['embeds'])))
                    if int(message['id']) > last_message_id:
                        last_message_id = int(message['id'])
                success = insert(querys['message'], messages_to_insert, db, cursor)
                if not success:
                    break
                if len(messages) < 100:
                    break
            # print(f"        \\ + {m} to {New.messages}/{Totals.messages + New.messages}")
    cursor.close()
    db.close()
    Totals.guilds += New.guilds
    Totals.channels += New.channels
    Totals.users += New.users
    Totals.messages += New.messages
    Totals.new_messages += New.messages
    print(
        f"Bot {name} done. Guilds: {New.guilds}/{Totals.guilds} Channels: {New.channels}/{Totals.channels} Messages: {New.messages}/{Totals.messages} Users: {New.users}/{Totals.users}")
    print(
        f"Bot {name} done. Time elapsed: {round(time() - start_time, 2)} seconds  New Messages/Second: {round(New.messages / (time() - start_time), 2)}")


if __name__ == '__main__':
    processes = []
    START_TIME = time()
    init_db()
    with open("creds.json", 'r') as creds_json:
        creds = load(creds_json)
    for t in creds:
        if t == "end":
            break
        p = Process(target=dump, args=(t, creds[t]))
        p.start()
        processes.append(p)
    # for t in creds['user']:
    #     p = Process(target=dump, args=(t, creds['user'][t], False))
    #     p.start()
    #     processes.append(p)
    while True:
        for p in processes:
            if p.is_alive():
                continue
            else:
                p.join()
                processes.remove(p)
                print(f"Processes remaining: {len(processes)}")
        if len(processes) == 0:
            break
    print(f"Total time elapsed: {round(time() - START_TIME, 2)}")
    print(
        f"Guilds: {Totals.guilds} Channels: {Totals.channels} Messages: +{Totals.new_messages} > {Totals.messages} Users: {Totals.users}")
