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
import concurrent.futures
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mtick
import os  # Add this to the imports section

########## DEFINES ##########

# Color Palette
PRIMARY_COLOR = '#FFA500'  # Orange
SECONDARY_COLOR = '#800080'  # Purple

stat_headers = {
    'name': 'Person',
    'count': 'Total Messages',
    'chars': 'Total Characters',
    'words': 'Total Words',
    'avg_chars': 'Avg Chars Per Msg',
    'avg_words': 'Avg Words Per Msg',
    'avg_word_len': 'Avg Chars Per Word',
    'emoji': 'Total Emoji Count',
    'reactions': 'Total Reaction Count',
    'attachments': 'Total Attachment Count',
    'avg_delta': 'Avg Reply Time (s)',
    'avg_spec_delta': 'Avg Direct Reply Time (s)',
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

attachment_headers = {
    'gif' : 'GIF',
    'pic' : 'Picture',
    'vid' : 'Video',
    'aud' : 'Audio',
    'loc' : 'Location'
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

    chat_ids = [h['chat_id'] for h in chat2handles if h['handle_id'] in handle_ids]
    
    attc_dict = dict()
    for attc in attachments:
        attc_dict[attc['message_id']] = attc_dict.get(attc['message_id'], []) + [attc]

    messages = sorted([m for m in messages if m['chat_id'] in chat_ids], key=lambda d: d['date'])
    for msg in messages:
        msg['attachments'] = attc_dict.get(msg['rowid'], [])

    return handle_ids, messages

def export_to_csv(data, filename, headers = None):
    df = pandas.DataFrame(data).fillna(0)
    
    if headers:
        df = df.rename(columns=headers)

    df.to_csv(filename, index=False)

def create_wordcloud(thecolor, thefile, thetext, themask):
    WordCloud(background_color="white", max_words=2000, mask=themask, contour_width=0, colormap=thecolor, min_word_length=3).generate(thetext).to_file(thefile)

def generate_graph(data, filename, headers=None, group_by_month=False):
    """Generates a graph showing reaction counts with grouped bars and a total line."""
    df = pandas.DataFrame(data) if isinstance(data, list) else data
    if headers:
        df = df.rename(columns=headers)

    # Convert numeric columns to float
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].astype(float)

    # Create figure with enough height for graph and table
    fig = plt.figure(figsize=(12, 8))
    gs = fig.add_gridspec(2, 1, height_ratios=[3, 1])
    ax1 = fig.add_subplot(gs[0])
    
    if len(df) > 1:
        categories = df.columns[1:]
        numeric_categories = [col for col in categories if col in numeric_cols]
        x = np.arange(len(numeric_categories))
        width = 0.35
        
        # Create bars
        bars1 = ax1.bar(x - width/2, df.iloc[0][numeric_categories].values, width,
                       label=df.iloc[0,0], color=PRIMARY_COLOR, alpha=0.8)
        bars2 = ax1.bar(x + width/2, df.iloc[1][numeric_categories].values, width,
                       label=df.iloc[1,0], color=SECONDARY_COLOR, alpha=0.8)
        
        # Add total line
        totals = df.iloc[:-1][numeric_categories].sum()
        ax1.plot(x, totals, color='gray', alpha=0.3, linewidth=2,
                label='Total', marker='o')
        ax1.fill_between(x, totals, alpha=0.1, color='gray')
        
        # Customize axes
        ax1.set_xticks(x)
        ax1.set_xticklabels(numeric_categories, rotation=45, ha='right')
        ax1.set_ylabel('Count')
        
        # Add value labels on bars
        def autolabel(bars):
            for bar in bars:
                height = bar.get_height()
                ax1.annotate(f'{int(height)}',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha='center', va='bottom')
        
        autolabel(bars1)
        autolabel(bars2)
        ax1.legend()
        
        # Add table with flipped orientation
        ax_table = fig.add_subplot(gs[1])
        ax_table.axis('off')
        
        # Prepare table data with categories as columns
        table_data = []
        for cat in numeric_categories:
            row = [cat, f"{df.iloc[0][cat]}", f"{df.iloc[1][cat]}", f"{totals[cat]}"]
            table_data.append(row)
        
        table = ax_table.table(cellText=table_data,
                             colLabels=['Category', df.iloc[0,0], df.iloc[1,0], 'Total'],
                             cellLoc='center',
                             loc='center',
                             bbox=[0, 0, 1, 1])
        
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1.2, 1.5)
        
        plt.tight_layout()
    
    plt.savefig(filename, bbox_inches='tight', dpi=300)
    plt.close()


def main():
    parser = argparse.ArgumentParser(description='Process iMessage data.')
    parser.add_argument('-u', '--update-data', action='store_true', help='Re-run the iMessage data export')
    parser.add_argument('-p', '--phone-number', type=str, help='Phone number to analyze', required=True)
    parser.add_argument('-s', '--start-date', type=str, help='Earliest message date to include', required=False, default='1990-1-1')
    parser.add_argument('-e', '--end-date', type=str, help='Latest message date to include', required=False, default='2111-1-1')
    parser.add_argument('-o', '--output-file', type=str, help='Output file for thread formatting', required=False, default='thread_output.txt')
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
    threads = dict()
    non_threaded_messages = []
    print(f"\tFiltered to {len(filtered_messages):,d} messages between {start_date.date()} and {end_date.date()}.")

    # Build threads dictionary and identify non-threaded messages
    processed_message_ids = set()
    for msg in filtered_messages:
        if msg['thread_originator_guid']:
            threads[msg['thread_originator_guid']] = threads.get(msg['thread_originator_guid'], []) + [msg]
            processed_message_ids.add(msg['rowid'])
        else:
            # If a message has no thread_originator_guid, it might be a thread starter
            # We'll check if it's truly standalone later
            threads[msg['guid']] = [msg]
            processed_message_ids.add(msg['rowid'])
    
    # Find any messages that weren't included in threads
    for msg in filtered_messages:
        if msg['rowid'] not in processed_message_ids:
            non_threaded_messages.append(msg)
    
    # Helper function to format a list of messages
    def format_messages(message_list, file):
        # Sort messages by date
        message_list.sort(key=lambda m: m['date'])
        
        # Process each message
        for msg in message_list:
            # Skip reaction messages
            if msg['text'] and any(msg['text'].startswith(pattern) for pattern in 
                                  ['Liked', 'Disliked', 'Loved', 'Laughed', 'Questioned', 'Emphasized']):
                continue
            
            # Determine sender
            sender = "Warren" if msg['is_from_me'] else "Irelyn"
            
            # Format message content
            if msg['attachments']:
                attachment_types = []
                for attc in msg['attachments']:
                    if attc['mime_type']:
                        if attc['mime_type'].startswith('image/gif'):
                            attachment_types.append("<gif>")
                        elif attc['mime_type'].startswith('image'):
                            attachment_types.append("<image>")
                        elif attc['mime_type'].startswith('video'):
                            attachment_types.append("<video>")
                        elif attc['mime_type'].startswith('audio'):
                            attachment_types.append("<audio>")
                        elif attc['mime_type'].startswith('text/x-vlocation'):
                            attachment_types.append("<location>")
                        else:
                            attachment_types.append(f"<{attc['mime_type']}>")
                content = " ".join(attachment_types)
            else:
                content = msg['text'] if msg['text'] else ""
            
            # Clean any unprintable characters
            content = "".join(c if c.isprintable() else " " for c in content)
            
            # Write formatted message
            file.write(f"[{sender}]: {content}\n")

    # Format and output threads
    current_file_index = 1
    current_file = None
    MAX_FILE_SIZE = 500000  # 500KB

    def get_output_file_name(base_name, index):
        name, ext = os.path.splitext(base_name)
        return f"{name}_{index}{ext}"

    def write_thread(thread_content):
        nonlocal current_file, current_file_index
        
        # Calculate content size
        content_size = len(thread_content.encode('utf-8'))
        
        # If we need a new file
        if current_file is None or current_file.tell() + content_size > MAX_FILE_SIZE:
            if current_file is not None:
                current_file.close()
            current_file = open(get_output_file_name(args.output_file, current_file_index), 'w', encoding='utf-8')
            current_file_index += 1
        
        current_file.write(thread_content)

    try:
        # Process normal threads
        for idx, (thread_id, messages) in enumerate(threads.items(), 1):
            if len(messages) <= 1:
                non_threaded_messages.extend(messages)
                continue
            
            # Build complete thread content in memory
            thread_content = [f"Thread Start: {idx}\n\n"]
            
            # Sort messages by date
            messages.sort(key=lambda m: m['date'])
            
            # Process each message
            for msg in messages:
                if msg['text'] and any(msg['text'].startswith(pattern) for pattern in 
                                    ['Liked', 'Disliked', 'Loved', 'Laughed', 'Questioned', 'Emphasized']):
                    continue
                
                sender = "Warren" if msg['is_from_me'] else "Irelyn"
                
                if msg['attachments']:
                    attachment_types = []
                    for attc in msg['attachments']:
                        if attc['mime_type']:
                            if attc['mime_type'].startswith('image/gif'):
                                attachment_types.append("<gif>")
                            elif attc['mime_type'].startswith('image'):
                                attachment_types.append("<image>")
                            elif attc['mime_type'].startswith('video'):
                                attachment_types.append("<video>")
                            elif attc['mime_type'].startswith('audio'):
                                attachment_types.append("<audio>")
                            elif attc['mime_type'].startswith('text/x-vlocation'):
                                attachment_types.append("<location>")
                            else:
                                attachment_types.append(f"<{attc['mime_type']}>")
                    content = " ".join(attachment_types)
                else:
                    content = msg['text'] if msg['text'] else ""
                
                content = "".join(c if c.isprintable() else " " for c in content)
                thread_content.append(f"[{sender}]: {content}\n")
            
            thread_content.append("--- Thread End ---\n\n\n")
            write_thread("".join(thread_content))

        # Process non-threaded messages
        if non_threaded_messages:
            non_threaded_messages.sort(key=lambda m: m['date'])
            standalone_threads = []
            current_thread = []
            last_time = None
            
            for msg in non_threaded_messages:
                msg_time = unix_epoch + datetime.timedelta(seconds=msg['date'] / 10**9)
                
                if not last_time or (msg_time - last_time).total_seconds() < 3600:
                    current_thread.append(msg)
                else:
                    standalone_threads.append(current_thread)
                    current_thread = [msg]
                
                last_time = msg_time
            
            if current_thread:
                standalone_threads.append(current_thread)
            
            for i, thread in enumerate(standalone_threads, len(threads) + 1):
                thread_content = [f"Thread Start: {i}\n\n"]
                thread.sort(key=lambda m: m['date'])
                
                for msg in thread:
                    if msg['text'] and any(msg['text'].startswith(pattern) for pattern in 
                                        ['Liked', 'Disliked', 'Loved', 'Laughed', 'Questioned', 'Emphasized']):
                        continue
                    
                    sender = "Warren" if msg['is_from_me'] else "Irelyn"
                    
                    if msg['attachments']:
                        attachment_types = []
                        for attc in msg['attachments']:
                            if attc['mime_type']:
                                if attc['mime_type'].startswith('image/gif'):
                                    attachment_types.append("<gif>")
                                elif attc['mime_type'].startswith('image'):
                                    attachment_types.append("<image>")
                                elif attc['mime_type'].startswith('video'):
                                    attachment_types.append("<video>")
                                elif attc['mime_type'].startswith('audio'):
                                    attachment_types.append("<audio>")
                                elif attc['mime_type'].startswith('text/x-vlocation'):
                                    attachment_types.append("<location>")
                                else:
                                    attachment_types.append(f"<{attc['mime_type']}>")
                        content = " ".join(attachment_types)
                    else:
                        content = msg['text'] if msg['text'] else ""
                    
                    content = "".join(c if c.isprintable() else " " for c in content)
                    thread_content.append(f"[{sender}]: {content}\n")
                
                thread_content.append("Thread End\n\n")
                write_thread("".join(thread_content))

    finally:
        if current_file:
            current_file.close()

    print(f"Thread formatting completed. Output written to {args.output_file}_1 through {args.output_file}_{current_file_index-1}")
    print(f"Processed {len(threads)} threaded conversations and {len(standalone_threads) if 'standalone_threads' in locals() else 0} standalone threads")


if __name__ == "__main__":
    main()