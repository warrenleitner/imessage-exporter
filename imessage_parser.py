# Import the json module
import json
import re
from tabulate import tabulate, SEPARATING_LINE
import subprocess
import datetime
from zoneinfo import ZoneInfo
import itertools
import pandas
import emoji


# Define the MacOS epoch
unix_epoch = datetime.datetime(2001, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("America/Los_Angeles"))

# subprocess.run(["cargo", "run", "--release", "--bin", "imessage-stats"])


# Open the json file and read its contents
with open("attachments.json", "r") as f:
    data = f.read()
attachments = json.loads(data)

with open("chat2handles.json", "r") as f:
    data = f.read()
chat2handles = json.loads(data)

with open("chats.json", "r") as f:
    data = f.read()
chats = json.loads(data)

with open("handles.json", "r") as f:
    data = f.read()
handles = json.loads(data)

with open("messages.json", "r") as f:
    data = f.read()
messages = json.loads(data)

irelyns_phone = "17028811404"
handle_ids = [h['rowid'] for h in handles if h['id'].find(irelyns_phone) != -1]
print('Handles: ' + str(handle_ids))

chat_ids = [h['chat_id'] for h in chat2handles if h['handle_id'] in handle_ids]
print('Chats: ' + str(chat_ids))

filtered_messages = sorted([m for m in messages if m['chat_id'] in chat_ids], key=lambda d: d['date'])
print(len(filtered_messages))

# Define the Unix epoch
unix_epoch = datetime.datetime(2001, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("America/Los_Angeles"))

# Add the seconds to the Unix epoch
first_msg_time = unix_epoch + datetime.timedelta(seconds=filtered_messages[0]['date'] / 10**9)
last_msg_time = unix_epoch + datetime.timedelta(seconds=filtered_messages[-1]['date'] / 10**9)

IRELYN_IDX=0
WARREN_IDX=1
TOTAL_IDX=2
MY_INDICIES=[IRELYN_IDX, WARREN_IDX, TOTAL_IDX]
MY_NAMES=['Irelyn', 'Warren', 'TOTAL']

template_dict = {'name': "", 'count': 0, 'chars': 0, 'words': 0, 'emoji': 0}
stats = [dict(template_dict), dict(template_dict), dict(template_dict)]

time_of_day_counters = [dict(), dict(), dict()]
day_of_week_counters = [dict(), dict(), dict()]
date_counters = [dict(), dict(), dict()]
emoji_counters = [dict(), dict(), dict()]

for i,d in itertools.product(MY_INDICIES, [stats, time_of_day_counters, day_of_week_counters, date_counters, emoji_counters]):
    d[i]['name'] = MY_NAMES[i]

for i,cnt in itertools.product(range(0, 24), time_of_day_counters):
    cnt[i] = 0

for i,cnt in itertools.product(range(0, 7), day_of_week_counters):
    cnt[i] = 0

for msg in filtered_messages:
    if msg['is_from_me']:
        idx = WARREN_IDX
    elif msg['handle_id'] in handle_ids:
        idx = IRELYN_IDX
    else:
        print("Unknown ID: " + str(msg['handle_id']))
        continue

    stats[idx]['count'] = stats[idx]['count'] + 1
    if msg['text']:
        stats[idx]['chars'] = stats[idx]['chars'] + len(msg['text'])
        stats[idx]['words'] = stats[idx]['words'] + len(re.findall(r"\w+", msg['text']))
        for emj in ''.join(c for c in msg['text'] if c in emoji.EMOJI_DATA):
            stats[idx]['emoji'] = stats[idx]['emoji'] + 1
            emoji_counters[idx][emj] = emoji_counters[idx].get(emj, 0) + 1
    
    # Add the seconds to the Unix epoch
    msg_time = unix_epoch + datetime.timedelta(seconds=msg['date'] / 10**9)
    time_of_day_counters[idx][msg_time.hour] = time_of_day_counters[idx][msg_time.hour] + 1
    day_of_week_counters[idx][msg_time.weekday()] = day_of_week_counters[idx][msg_time.weekday()] + 1
    str_date = str((msg_time - datetime.timedelta(days=msg_time.weekday())).date())
    date_counters[idx][str_date] = date_counters[idx].get(str_date, 0) + 1

for key in ['count', 'chars', 'words', 'emoji']:
    stats[TOTAL_IDX][key] = sum(d.get(key, 0) for d in stats[:TOTAL_IDX])

for stat in stats:
    stat['avg_chars'] = int(stat['chars'] / stat['count'])
    stat['avg_words'] = int(stat['words'] / stat['count'])
    stat['avg_word_len'] = float(stat['chars'] / stat['words'])

for i in range(0, 24): 
    time_of_day_counters[TOTAL_IDX][i] = sum(d.get(i, 0) for d in time_of_day_counters[:TOTAL_IDX]) 

for i in range(0, 7):
    day_of_week_counters[TOTAL_IDX][i] = sum(d.get(i, 0) for d in day_of_week_counters[:TOTAL_IDX]) 

for i in pandas.date_range(first_msg_time, last_msg_time, freq='1W'):
    str_date = str((i - datetime.timedelta(days=msg_time.weekday())).date())
    date_counters[TOTAL_IDX][str_date] = sum(d.get(str_date, 0) for d in date_counters[:TOTAL_IDX]) 

for i in list(set.union(*(set(d.keys()) for d in emoji_counters[:TOTAL_IDX])).difference(['name'])):
    emoji_counters[TOTAL_IDX][i] = sum(d.get(i, 0) for d in emoji_counters[:TOTAL_IDX])
    for d in emoji_counters[:TOTAL_IDX]: d[i] = d.get(i, 0)
    if emoji_counters[TOTAL_IDX][i] < stats[TOTAL_IDX]['emoji'] * .01:
        #print(f"Emoji Threshold: {stats[TOTAL_IDX]['emoji'] * .01:.0f}")
        for j in MY_INDICIES:
            emoji_counters[j]['other'] = emoji_counters[j].get('other', 0) + emoji_counters[j].pop(i)


stat_headers = {
    'name': 'Person',
    'count': 'Total Messages',
    'chars': 'Total Characters',
    'words': 'Total Words',
    'avg_chars': 'Avg Chars Per Msg',
    'avg_words': 'Avg Words Per Msg',
    'avg_word_len': 'Avg Chars Per Word',
    'emoji': 'Total Emoji Count' 
}

day_headers = {
    'name': 'Person',
    0: 'Monday',
    1: 'Tuesday',
    2: 'Wendesday',
    3: 'Thursday',
    4: 'Friday',
    5: 'Saturday',
    6: 'Sunday',
}

BIBLE_WORDS = 783137
FACEBOOK_WORDS = 23000
date_diff = last_msg_time - first_msg_time
date_diff_days = date_diff.total_seconds() / 60 / 60 / 24
words_per_day = (stats[-1]["words"] + FACEBOOK_WORDS) / date_diff_days
days_remaining = (BIBLE_WORDS - stats[-1]["words"] - FACEBOOK_WORDS) / words_per_day
bible_date = datetime.datetime.now() + datetime.timedelta(days = days_remaining)

print("")
print(tabulate(stats[:TOTAL_IDX] + [{"name": SEPARATING_LINE}, stats[-1]], headers=stat_headers, intfmt=",", floatfmt=".2f", tablefmt="simple"))

print("")
msgs_per_day = stats[-1]["count"] / date_diff_days
print(f'- Messages Per Day: {msgs_per_day:,.0f}')
print(f'- First Message Date: {first_msg_time:%A, %B %d, %Y at %I:%M:%S %p %Z}')
print(f'- Most Recent Message Date: {last_msg_time:%A, %B %d, %Y at %I:%M:%S %p %Z}')
print(f'- Percent of King James Bible: {float(100 * (stats[-1]["words"] + FACEBOOK_WORDS) / BIBLE_WORDS):.2f}%')
print(f'- At the current average rate of {words_per_day:,.0f} words per day, it will take us {days_remaining:.0f} days to finish writing our bible, and it will be complete on {bible_date:%A, %B %d, %Y}.')

end_date = datetime.datetime(1994 + 109, 5, 27, 11, 21, 0, tzinfo=ZoneInfo("America/New_York"))
days_remaining = (end_date - datetime.datetime.now().replace(tzinfo=ZoneInfo("America/Los_Angeles"))).days
print(f'- In the {days_remaining / 365.25:.2f} years remaining in our relationship, we will write {msgs_per_day * days_remaining:,.0f} more messages and {days_remaining * words_per_day:,.0f} more words, or {days_remaining * words_per_day / BIBLE_WORDS:,.1f} bibles')

print("")
print(tabulate(time_of_day_counters[:TOTAL_IDX] + [{"name": SEPARATING_LINE}, time_of_day_counters[-1]], headers="keys", intfmt=",", floatfmt=".2f", tablefmt="simple"))

print("")
print(tabulate(day_of_week_counters[:TOTAL_IDX] + [{"name": SEPARATING_LINE}, day_of_week_counters[-1]], headers=day_headers, intfmt=",", floatfmt=".2f", tablefmt="simple"))

date_counters_df = pandas.DataFrame(date_counters).fillna(0).apply(pandas.to_numeric, downcast='integer', errors='ignore').transpose().sort_index()
date_counters_df = date_counters_df.rename(columns=date_counters_df.iloc[-1]).drop(date_counters_df.index[-1])
pandas.set_option('display.max_columns', None)
print("")
print(date_counters_df.transpose().to_csv())
#print(tabulate(date_counters[:TOTAL_IDX] + [{"name": SEPARATING_LINE}, date_counters[-1]], headers="keys", intfmt=",", floatfmt=".2f", tablefmt="simple"))

print("")
print(tabulate(emoji_counters[:TOTAL_IDX] + [{"name": SEPARATING_LINE}, emoji_counters[-1]], headers="keys", intfmt=",", floatfmt=".2f", tablefmt="simple"))

# First message count
# Last message count
# Average response time
# Total attachments
# Types of attachments
# Average individual message reply time
# Reaction Stats
# DONE Peak time of day (graph)
# DONE Peak day of week (graph)
# DONE Date graph
# DONE Emoji usage
# DONE Most used emojis
# Word cloud
# Stats pre and post break


# Parse and splice facebook messages


# Aggregate stats over time????
