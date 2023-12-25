
extern crate imessage_database;
extern crate serde_json;

use imessage_database::{
    error::table::TableError,
    tables::{
        messages::Message,
        chat::Chat,
        handle::Handle,
        chat_handle::ChatToHandle,
        attachment::Attachment,
        table::{get_connection, Table},
    },
    util::dirs::default_db_path,
};

use serde_json::json;

fn iter_messages() -> Result<(), TableError> {
    /// Create a read-only connection to an iMessage database
    let db = get_connection(&default_db_path()).unwrap();

    /// Create SQL statement
    let mut statement = Message::get(&db)?;

    // Execute statement
    let messages = statement
        .query_map([], |row| Ok(Message::from_row(row)))
        .unwrap();

    /// Iterate over each row
    println!("vvvvvvvvvv MESSAGES START HERE vvvvvvvvvv");
    let mut messages_vec = Vec::new();
    let mut attachments_vec = Vec::new();
    for message in messages {
        let mut msg = Message::extract(message)?;

        /// Parse message body if it was sent from macOS 13.0 or newer
        msg.gen_text(&db);

        let mut msg_attcs = Attachment::from_message(&db, &msg);
        if msg_attcs.is_ok() {
            for mut attc in msg_attcs.unwrap() {
                attc.message_id = msg.rowid;
                attachments_vec.push(attc);
            }
        }
        messages_vec.push(json!(msg));
    }
    std::fs::write("messages.json", serde_json::to_string_pretty(&messages_vec).unwrap());
    drop(messages_vec);
    std::fs::write("attachments.json", serde_json::to_string_pretty(&attachments_vec).unwrap());
    drop(attachments_vec);
    println!("^^^^^^^^^  MESSAGES END HERE  ^^^^^^^^^");

    let mut statement = Chat::get(&db)?;
    let chats = statement
        .query_map([], |row| Ok(Chat::from_row(row)))
        .unwrap();

    /// Iterate over each row
    println!("vvvvvvvvvv CHATS START HERE vvvvvvvvvv");
    let mut chats_vec = Vec::new();
    for chat in chats {
        let mut cht = Chat::extract(chat)?;

        chats_vec.push(json!(cht));
    }
    std::fs::write("chats.json", serde_json::to_string_pretty(&chats_vec).unwrap());
    drop(chats_vec);
    println!("^^^^^^^^^  CHATS END HERE  ^^^^^^^^^");

    println!("vvvvvvvvvv HANDLES START HERE vvvvvvvvvv");
    let mut handles_vec = Vec::new();
    let mut statement = Handle::get(&db)?;
    let handles = statement
        .query_map([], |row| Ok(Handle::from_row(row)))
        .unwrap();

    /// Iterate over each row
    for handle in handles {
        let mut hndl = Handle::extract(handle)?;

        handles_vec.push(json!(hndl));
    }
    std::fs::write("handles.json", serde_json::to_string_pretty(&handles_vec).unwrap());
    drop(handles_vec);
    println!("^^^^^^^^^  HANDLES END HERE  ^^^^^^^^^");

    // Does not define a debug format
    println!("vvvvvvvvvv CHATTOHANDLES START HERE vvvvvvvvvv");
    let mut chat2handle_vec = Vec::new();
    let mut statement = ChatToHandle::get(&db)?;
    let chattohandles = statement
        .query_map([], |row| Ok(ChatToHandle::from_row(row)))
        .unwrap();

    /// Iterate over each row
    for chathandle in chattohandles {
        let mut c2h = ChatToHandle::extract(chathandle)?;

        chat2handle_vec.push(json!(c2h));
    }
    std::fs::write("chat2handles.json", serde_json::to_string_pretty(&chat2handle_vec).unwrap());
    drop(chat2handle_vec);
    println!("^^^^^^^^^  CHATTOHANDLES END HERE  ^^^^^^^^^");

    Ok(())
}

fn main() {
    let ret = iter_messages();
    if(ret.is_err()) {
        println!("{}", ret.unwrap_err());
    }
}