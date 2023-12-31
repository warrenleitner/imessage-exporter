

########## IMPORTS ##########
import json
import re
import subprocess
import datetime
from zoneinfo import ZoneInfo
import itertools
import pandas
import emoji
from PIL import Image
import numpy as np
from wordcloud import WordCloud
import argparse
import time

########## DEFINES ##########
stat_headers = {
    'name': 'Person',
    'count': 'Total Messages',
    'chars': 'Total Characters',
    'words': 'Total Words',
    'avg_chars': 'Avg Chars Per Msg',
    'avg_words': 'Avg Words Per Msg',
    'avg_word_len': 'Avg Chars Per Word',
    'emoji': 'Total Emoji Count',
    'reactions': 'Total Reaction Count'
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

reaction_headers = {
    'name' : 'Person',
    'Liked': '👍',
    'Disliked': '👎',
    'Laughed': '😆',
    'Loved': '❤️',
    'Questioned': '❓',
    'Emphasized': '❗️'
}

def load_json_data(filename):
    with open(filename, "r") as f:
        data = f.read()
    return json.loads(data)

def load_messages(phone_number):
    attachments = load_json_data("attachments.json")
    chat2handles = load_json_data("chat2handles.json")
    chats = load_json_data("chats.json")
    handles = load_json_data("handles.json")
    messages = load_json_data("messages.json")
    print(f"\tProcessed {len(attachments) + len(chat2handles) + len(chats) + len(handles) + len(messages):,d} total records.")

    handle_ids = [h['rowid'] for h in handles if h['id'].find(phone_number) != -1]
    # print('Handles: ' + str(handle_ids))

    chat_ids = [h['chat_id'] for h in chat2handles if h['handle_id'] in handle_ids]
    # print('Chats: ' + str(chat_ids))

    return handle_ids, sorted([m for m in messages if m['chat_id'] in chat_ids], key=lambda d: d['date'])

def export_to_csv(data, filename, headers = None):
    df = pandas.DataFrame(data).fillna(0)
    
    if headers:
        df = df.rename(columns=headers)

    df.to_csv(filename, index=False)

def main():
    parser = argparse.ArgumentParser(description='Process iMessage data.')
    parser.add_argument('-u', '--update-data', action='store_true', help='Re-run the iMessage data export')
    parser.add_argument('-p', '--phone-number', type=str, help='Phone number to analyze', required=True)
    parser.add_argument('-s', '--start-date', type=str, help='Earliest message date to include', required=False, default='1990-1-1')
    parser.add_argument('-e', '--end-date', type=str, help='Latest message date to include', required=False, default='2111-1-1')
    args = parser.parse_args()

    if args.update_data:
        print("Compiling and running imessage-exporter...")
        segment_start = time.time()
        subprocess.run(["cargo", "run", "--release", "--bin", "imessage-stats"], stdout=subprocess.DEVNULL)
        print(f"Done! Took {time.time() - segment_start:.2f} seconds.\n")

    # Define the MacOS epoch
    unix_epoch = datetime.datetime(2001, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("America/Los_Angeles"))
    
    print(f'Loading messages for phone number {args.phone_number}...')
    segment_start = time.time()
    handle_ids, filtered_messages = load_messages(args.phone_number)
    print(f"\tFound {len(filtered_messages):,d} messages for phone number {args.phone_number}")

    # Remove messages outside the bounds of start_date and end_date
    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=ZoneInfo("America/Los_Angeles"))
    end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=ZoneInfo("America/Los_Angeles"))
    filtered_messages = [msg for msg in filtered_messages if start_date <= (unix_epoch + datetime.timedelta(seconds=msg['date'] / 10**9)) <= end_date]
    print(f"\tFiltered to {len(filtered_messages):,d} messages between {start_date.date()} and {end_date.date()}.")

    # Define the Unix epoch
    unix_epoch = datetime.datetime(2001, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("America/Los_Angeles"))

    # Add the seconds to the Unix epoch
    first_msg_time = unix_epoch + datetime.timedelta(seconds=filtered_messages[0]['date'] / 10**9)
    last_msg_time = unix_epoch + datetime.timedelta(seconds=filtered_messages[-1]['date'] / 10**9)
    end_date = datetime.datetime(1994 + 109, 5, 27, 8, 21, 0, tzinfo=ZoneInfo("America/Los_Angeles"))
    our_days_remaining = (end_date - datetime.datetime.now().replace(tzinfo=ZoneInfo("America/Los_Angeles"))).days

    IRELYN_IDX=0
    WARREN_IDX=1
    TOTAL_IDX=2
    MY_INDICIES=[IRELYN_IDX, WARREN_IDX, TOTAL_IDX]
    MY_NAMES=['Irelyn', 'Warren', 'TOTAL']

    template_dict = {'name': "", 'count': 0, 'chars': 0, 'words': 0, 'emoji': 0, 'reactions': 0}
    stats = [dict(template_dict), dict(template_dict), dict(template_dict)]

    time_of_day_counters = [dict(), dict(), dict()]
    day_of_week_counters = [dict(), dict(), dict()]
    date_counters = [dict(), dict(), dict()]
    emoji_counters = [dict(), dict(), dict()]
    reaction_counters = [dict(), dict(), dict()]

    wordcloud_text = ["", "", ""]

    REACTION_TYPES = ['Liked', 'Disliked', 'Loved', 'Laughed', 'Questioned', 'Emphasized']

    for i,d in itertools.product(MY_INDICIES, [stats, time_of_day_counters, day_of_week_counters, date_counters, emoji_counters, reaction_counters]):
        d[i]['name'] = MY_NAMES[i]

    for i,cnt in itertools.product(range(0, 24), time_of_day_counters):
        cnt[i] = 0

    for i,cnt in itertools.product(range(0, 7), day_of_week_counters):
        cnt[i] = 0

    for i,cnt in itertools.product(REACTION_TYPES, reaction_counters):
        cnt[i] = 0

    print(f"Done! Took {time.time() - segment_start:.2f} seconds.\n")

    print(f'Parsing messages...')
    segment_start = time.time()
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
            for pattern in REACTION_TYPES:
                if msg['text'].startswith(pattern):
                    stats[idx]['reactions'] += 1
                    reaction_counters[idx][pattern] += 1
                    break
            else:
                wordcloud_text[idx] += " " + msg['text'].replace("’", "").lower()
                stats[idx]['chars'] += len(msg['text'])
                stats[idx]['words'] += len(re.findall(r"\w+", msg['text']))
                for emj in ''.join(c for c in msg['text'] if c in emoji.EMOJI_DATA):
                    stats[idx]['emoji'] += 1
                    emoji_counters[idx][emj] = emoji_counters[idx].get(emj, 0) + 1

        # Add the seconds to the Unix epoch
        msg_time = unix_epoch + datetime.timedelta(seconds=msg['date'] / 10**9)
        time_of_day_counters[idx][msg_time.hour] = time_of_day_counters[idx][msg_time.hour] + 1
        day_of_week_counters[idx][msg_time.weekday()] = day_of_week_counters[idx][msg_time.weekday()] + 1
        str_date = str((msg_time - datetime.timedelta(days=msg_time.weekday())).date())
        date_counters[idx][str_date] = date_counters[idx].get(str_date, 0) + 1

    for key in ['count', 'chars', 'words', 'emoji', 'reactions']:
        stats[TOTAL_IDX][key] = sum(d.get(key, 0) for d in stats[:TOTAL_IDX])

    wordcloud_text[TOTAL_IDX] = wordcloud_text[WARREN_IDX] + " " + wordcloud_text[IRELYN_IDX]

    for stat in stats:
        stat['avg_chars'] = int(stat['chars'] / stat['count'])
        stat['avg_words'] = int(stat['words'] / stat['count'])
        stat['avg_word_len'] = float(stat['chars'] / stat['words'])

    for i in range(0, 24): 
        time_of_day_counters[TOTAL_IDX][i] = sum(d.get(i, 0) for d in time_of_day_counters[:TOTAL_IDX]) 

    for i in range(0, 7):
        day_of_week_counters[TOTAL_IDX][i] = sum(d.get(i, 0) for d in day_of_week_counters[:TOTAL_IDX]) 

    for i in pandas.date_range(first_msg_time, end_date + datetime.timedelta(days=1), freq='1W', inclusive='both', normalize=True):
        str_date = str((i - datetime.timedelta(days=i.weekday())).date())
        date_counters[TOTAL_IDX][str_date] = sum(d.get(str_date, 0) for d in date_counters[:TOTAL_IDX]) 
    date_df = pandas.DataFrame(date_counters, columns=list(date_counters[TOTAL_IDX].keys())).fillna(0)

    for i in list(set.union(*(set(d.keys()) for d in emoji_counters[:TOTAL_IDX])).difference(['name'])):
        emoji_counters[TOTAL_IDX][i] = sum(d.get(i, 0) for d in emoji_counters[:TOTAL_IDX])
        for d in emoji_counters[:TOTAL_IDX]: d[i] = d.get(i, 0)

    emoji_df = pandas.DataFrame(emoji_counters).fillna(0).iloc[:,1:]    
    emoji_df = emoji_df.iloc[:, np.flip(np.argsort(emoji_df.loc[TOTAL_IDX]))].transpose()
    if emoji_df.shape[0] > 10:
        temp_df = pandas.DataFrame(emoji_df.iloc[10:].sum()).transpose()
        temp_df.index = ['Other']
        emoji_df = pandas.concat([emoji_df[:10], temp_df])
    emoji_df = emoji_df.transpose()
    emoji_df = pandas.concat([pandas.DataFrame(MY_NAMES, columns=['name']), emoji_df], axis=1)

    for i in REACTION_TYPES:
        reaction_counters[TOTAL_IDX][i] = sum(d.get(i, 0) for d in reaction_counters[:TOTAL_IDX]) 
    print(f"Done! Took {time.time() - segment_start:.2f} seconds.\n")

    print(f'Exporting tables...')
    segment_start = time.time()
    export_to_csv(stats, "stats.csv", headers=stat_headers)
    export_to_csv(time_of_day_counters, "time_of_day_counters.csv")
    export_to_csv(day_of_week_counters, "day_of_week_counters.csv", headers=day_headers)
    export_to_csv(date_df, "date_counters.csv")
    export_to_csv(emoji_df, "emoji_counters.csv")
    export_to_csv(reaction_counters, 'reaction_counters.csv', reaction_headers)
    print(f"Done! Took {time.time() - segment_start:.2f} seconds.\n")

    print("Creating wordclouds...")
    segment_start = time.time()
    llama_mask = np.array(Image.open("llama.jpg"))
    cloud_colors = [ 'cool', 'autumn', 'plasma' ]
    for i in MY_INDICIES:
        WordCloud(background_color="white", max_words=2000, mask=llama_mask, contour_width=0, colormap=cloud_colors[i], min_word_length=3).generate(wordcloud_text[i]).to_file(f'cloud_{MY_NAMES[i].lower()}.png')

    print(f"Done! Took {time.time() - segment_start:.2f} seconds.\n")

    BIBLE_WORDS =   783137
    FACEBOOK_WORDS = 23000
    date_diff = last_msg_time - first_msg_time
    date_diff_days = date_diff.total_seconds() / 60 / 60 / 24
    msgs_per_day = stats[-1]["count"] / date_diff_days
    words_per_day = (stats[-1]["words"] + FACEBOOK_WORDS) / date_diff_days
    days_remaining = (BIBLE_WORDS - stats[-1]["words"] - FACEBOOK_WORDS) / words_per_day
    bible_date = datetime.datetime.now() + datetime.timedelta(days = days_remaining)
    
    print("")
    print(f'- Messages Per Day: {msgs_per_day:,.0f}')
    print(f'- First Message Date: {first_msg_time:%A, %B %d, %Y at %I:%M:%S %p %Z}')
    print(f'- Most Recent Message Date: {last_msg_time:%A, %B %d, %Y at %I:%M:%S %p %Z}')
    print(f'- Percent of King James Bible: {float(100 * (stats[-1]["words"] + FACEBOOK_WORDS) / BIBLE_WORDS):.2f}%')
    print(f'- At the current average rate of {words_per_day:,.0f} words per day, it will take us {days_remaining:.0f} days to finish writing our bible, and it will be complete on {bible_date:%A, %B %d, %Y}.')
    print(f'- In the {our_days_remaining / 365.25:.2f} years remaining in our relationship, we will write {msgs_per_day * our_days_remaining:,.0f} more messages and {our_days_remaining * words_per_day:,.0f} more words, or {our_days_remaining * words_per_day / BIBLE_WORDS:,.1f} bibles')
    print("")


if __name__ == "__main__":
    main()

# First message count
# Last message count
# Average response time
# Total attachments
# Types of attachments
# Average individual message reply time
# DONE Reaction Stats
# DONE Peak time of day (graph)
# DONE Peak day of week (graph)
# DONE Date graph
# DONE Emoji usage
# DONE Most used emojis
# DONE Word cloud
# DONE Stats pre and post break
# Documentation

# Parse and splice facebook messages
# App
# Parameterize stuff
# Refactor into functions