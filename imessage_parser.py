# Import the json module
import json
import re
from tabulate import tabulate, SEPARATING_LINE
import subprocess
import datetime
from zoneinfo import ZoneInfo
import pandas as pd

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

messages_df = pd.DataFrame(filtered_messages)
messages_df.set_index('rowid')
messages_df['date'] = pd.to_datetime(messages_df['date'], origin='2001-1-1', unit='ns', utc=True).dt.tz_convert('America/Los_Angeles')
pd.set_option('display.max_columns', None)
#print(messages_df.iloc[[0,-1]].transpose())

names = ["Irelyn", "Warren"]

split_texts = { 
    "Warren": messages_df.loc[messages_df['is_from_me'] == True], 
    "Irelyn" : messages_df.loc[ (messages_df['is_from_me'] == False) & (messages_df['handle_id'].isin(handle_ids))]
}

stats_df = pd.DataFrame(columns=['name', 'count', 'chars', 'words'])

for idx in names:
    stats_df = pd.concat([pd.DataFrame([[
        idx,
        split_texts[idx].shape[0],
        split_texts[idx].loc[split_texts[idx]['text'].notna()]['text'].str.len().sum(),
        len(split_texts[idx].loc[split_texts[idx]['text'].notna()]['text'].str.findall(r"\w+"))
    ]], columns=stats_df.columns), stats_df], ignore_index=True)
stats_df = stats_df.convert_dtypes()

stats_df.set_index('name')
stats_df.loc['TOTAL'] = stats_df.sum(skipna=True, numeric_only=True)
stats_df['avg_chars'] = (stats_df['chars'] / stats_df['count'])
stats_df['avg_words'] = (stats_df['words'] / stats_df['count'])
stats_df['avg_word_len'] = (stats_df['chars'] / stats_df['words'])
stats_df.astype('F')

print(stats_df)


#print(stats_df.transpose())

# summary_dict = dict(template_dict)
# summary_dict['name'] = 'TOTAL'
# summary_dict['count'] = sum(d.get('count', 0) for d in stats)
# summary_dict['chars'] = sum(d.get('chars', 0) for d in stats)
# summary_dict['words'] = sum(d.get('words', 0) for d in stats)
# stats.append(summary_dict)

# for stat in stats:
#     stat['avg_chars'] = int(stat['chars'] / stat['count'])
#     stat['avg_words'] = int(stat['words'] / stat['count'])
#     stat['avg_word_len'] = float(stat['chars'] / stat['words'])

# header_dict = {
#     'name': 'Person',
#     'count': 'Total Messages',
#     'chars': 'Total Characters',
#     'words': 'Total Words',
#     'avg_chars': 'Avg Chars Per Msg',
#     'avg_words': 'Avg Words Per Msg',
#     'avg_word_len': 'Avg Chars Per Word'
# }

# # # Define the Unix epoch
# # unix_epoch = datetime.datetime(2001, 1, 1, 0, 0, 0, tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("America/Los_Angeles"))

# # # Add the seconds to the Unix epoch
# # first_msg_time = unix_epoch + datetime.timedelta(seconds=filtered_messages[0]['date'] / 10**9)
# # last_msg_time = unix_epoch + datetime.timedelta(seconds=filtered_messages[-1]['date'] / 10**9)

# print("")
# print(tabulate(stats[:2] + [{"name": SEPARATING_LINE}, {"name": SEPARATING_LINE}, stats[-1]], headers=header_dict, intfmt=",", floatfmt=".2f", tablefmt="outline"))
# print("")
# print(f'- First Message Date: {first_msg_time:%A, %B %d, %Y at %I:%M:%S %p %Z}')
# print(f'- Most Recent Message Date: {last_msg_time:%A, %B %d, %Y at %I:%M:%S %p %Z}')
# print(f'- Percent of King James Bible: {float(100 * (stats[-1]["words"] + 23000) / 783137):.2f}%')

# date_diff = datetime.datetime.now().replace(tzinfo=ZoneInfo("America/Los_Angeles")) - first_msg_time
# words_per_day = (stats[-1]["words"] + 23000) / (date_diff.total_seconds() / 60 / 60 / 24)
# days_remaining = (783137 - stats[-1]["words"] - 23000) / words_per_day
# end_date = datetime.datetime.now() + datetime.timedelta(days = days_remaining)
# print(f'- At the current average rate of {words_per_day:,.0f} words per day, it will take us {days_remaining:.0f} days to finish writing our bible, and it will be complete on {end_date:%A, %B %d, %Y}.')
# First message count
# Last message count
# Average response time
# Total attachments
# Types of attachments
# Average individual message reply time
# Peak time of day (graph)
# Peak day of week (graph)
# Emoji usage
# Stats pre and post break

# Parse and splice facebook messages


# Aggregate stats over time????
