/*!
 This module represents common (but not all) columns in the `handle` table.
*/

use rusqlite::{Connection, Error, Result, Row, Statement};
use std::collections::{HashMap, HashSet};

use crate::{
    error::table::TableError,
    tables::table::{Cacheable, Deduplicate, Diagnostic, Table, HANDLE, ME},
    util::output::{done_processing, processing},
};

use serde::{Deserialize, Serialize};

/// Represents a single row in the `handle` table.
#[derive(Debug, Serialize, Deserialize)]
pub struct Handle {
    pub rowid: i32,
    pub id: String,
    pub person_centric_id: Option<String>,
}

impl Table for Handle {
    fn from_row(row: &Row) -> Result<Handle> {
        Ok(Handle {
            rowid: row.get("rowid")?,
            id: row.get("id")?,
            person_centric_id: row.get("person_centric_id").unwrap_or(None),
        })
    }

    fn get(db: &Connection) -> Result<Statement, TableError> {
        db.prepare(&format!("SELECT * from {HANDLE}"))
            .map_err(TableError::Handle)
    }

    fn extract(handle: Result<Result<Self, Error>, Error>) -> Result<Self, TableError> {
        match handle {
            Ok(Ok(handle)) => Ok(handle),
            Err(why) | Ok(Err(why)) => Err(TableError::Handle(why)),
        }
    }
}

impl Cacheable for Handle {
    type K = i32;
    type V = String;
    /// Generate a `HashMap` for looking up contacts by their IDs, collapsing
    /// duplicate contacts to the same ID String regardless of service
    ///
    /// # Example:
    ///
    /// ```
    /// use imessage_database::util::dirs::default_db_path;
    /// use imessage_database::tables::table::{Cacheable, get_connection};
    /// use imessage_database::tables::handle::Handle;
    ///
    /// let db_path = default_db_path();
    /// let conn = get_connection(&db_path).unwrap();
    /// let chatrooms = Handle::cache(&conn);
    /// ```
    fn cache(db: &Connection) -> Result<HashMap<Self::K, Self::V>, TableError> {
        // Create cache for user IDs
        let mut map = HashMap::new();
        // Handle ID 0 is self in group chats
        map.insert(0, ME.to_string());

        // Create query
        let mut statement = Handle::get(db)?;

        // Execute query to build the Handles
        let handles = statement
            .query_map([], |row| Ok(Handle::from_row(row)))
            .map_err(TableError::Handle)?;

        // Iterate over the handles and update the map
        for handle in handles {
            let contact = Handle::extract(handle)?;
            map.insert(contact.rowid, contact.id);
        }

        // Condense contacts that share person_centric_id so their IDs map to the same strings
        let dupe_contacts = Handle::get_person_id_map(db)?;
        for contact in dupe_contacts {
            let (id, new) = contact;
            map.insert(id, new);
        }

        // Done!
        Ok(map)
    }
}

impl Deduplicate for Handle {
    type T = String;

    /// Given the initial set of duplicated handles, deduplicate them
    ///
    /// This returns a new hashmap that maps the real handle ID to a new deduplicated unique handle ID
    /// that represents a single handle for all of the deduplicate handles
    fn dedupe(duplicated_data: &HashMap<i32, Self::T>) -> HashMap<i32, i32> {
        let mut deduplicated_participants: HashMap<i32, i32> = HashMap::new();
        let mut participant_to_unique_participant_id: HashMap<Self::T, i32> = HashMap::new();

        // Build cache of each unique set of participants to a new identifier:
        let mut unique_participant_identifier = 0;
        for (participant_id, participant) in duplicated_data {
            if let Some(id) = participant_to_unique_participant_id.get(participant) {
                deduplicated_participants.insert(participant_id.to_owned(), id.to_owned());
            } else {
                participant_to_unique_participant_id
                    .insert(participant.to_owned(), unique_participant_identifier);
                deduplicated_participants
                    .insert(participant_id.to_owned(), unique_participant_identifier);
                unique_participant_identifier += 1;
            }
        }
        deduplicated_participants
    }
}

impl Diagnostic for Handle {
    /// Emit diagnostic data for the Handles table
    ///
    /// Get the number of handles that are duplicated
    /// The `person_centric_id` is used to map handles that represent the
    /// same contact across ids (numbers, emails, etc) and across
    /// services (iMessage, Jabber, iChat, SMS, etc)
    ///
    /// # Example:
    ///
    /// ```
    /// use imessage_database::util::dirs::default_db_path;
    /// use imessage_database::tables::table::{Diagnostic, get_connection};
    /// use imessage_database::tables::handle::Handle;
    ///
    /// let db_path = default_db_path();
    /// let conn = get_connection(&db_path).unwrap();
    /// Handle::run_diagnostic(&conn);
    /// ```
    fn run_diagnostic(db: &Connection) -> Result<(), TableError> {
        processing();
        let query = concat!(
            "SELECT COUNT(DISTINCT person_centric_id) ",
            "FROM handle ",
            "WHERE person_centric_id NOT NULL"
        );
        let mut rows = db.prepare(query).map_err(TableError::Messages)?;
        let count_dupes: Option<i32> = rows
            .query_row([], |r| r.get(0))
            .map_err(TableError::Handle)?;

        done_processing();

        if let Some(dupes) = count_dupes {
            if dupes > 0 {
                println!("\rContacts with more than one ID: {dupes}");
            }
        }
        Ok(())
    }
}

impl Handle {
    /// The handles table does not have a lot of information and can have many duplicate values.
    ///
    /// This method generates a hashmap of each separate item in this table to a combined string
    /// that represents all of the copies, so any handle ID will always map to the same string
    /// for a given chat participant
    fn get_person_id_map(db: &Connection) -> Result<HashMap<i32, String>, TableError> {
        let mut person_to_id: HashMap<String, HashSet<String>> = HashMap::new();
        let mut row_to_id: HashMap<i32, String> = HashMap::new();
        let mut row_data: Vec<(String, i32, String)> = vec![];

        // Build query
        let query = concat!(
            "SELECT DISTINCT A.person_centric_id, A.rowid, A.id ",
            "FROM handle A ",
            "INNER JOIN handle B ON B.id = A.id ",
            "WHERE A.person_centric_id NOT NULL ",
            "ORDER BY A.person_centric_id",
        );
        let statement = db.prepare(query);

        if let Ok(mut statement) = statement {
            // Cache the results of the query in memory
            let contacts = statement
                .query_map([], |row| {
                    let person_centric_id: String = row.get(0)?;
                    let rowid: i32 = row.get(1)?;
                    let id: String = row.get(2)?;
                    Ok((person_centric_id, rowid, id))
                })
                .map_err(TableError::Handle)?;

            for contact in contacts {
                match contact {
                    Ok(tup) => {
                        row_data.push(tup);
                    }
                    Err(why) => {
                        panic!("{why}");
                    }
                }
            }

            // First pass: generate a map of each person_centric_id to its matching ids
            for contact in &row_data {
                let (person_centric_id, _, id) = contact;
                if let Some(set) = person_to_id.get_mut(person_centric_id) {
                    set.insert(id.to_owned());
                } else {
                    let mut set = HashSet::new();
                    set.insert(id.to_owned());
                    person_to_id.insert(person_centric_id.to_owned(), set);
                }
            }

            // Second pass: point each ROWID to the matching ids
            for contact in &row_data {
                let (person_centric_id, rowid, _) = contact;
                let data_to_insert = match person_to_id.get_mut(person_centric_id) {
                    Some(person) => person.iter().cloned().collect::<Vec<String>>().join(" "),
                    None => panic!("Attempted to resolve contact with no person_centric_id!"),
                };
                row_to_id.insert(rowid.to_owned(), data_to_insert);
            }
        }

        Ok(row_to_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::tables::{handle::Handle, table::Deduplicate};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn test_can_dedupe() {
        let mut input: HashMap<i32, String> = HashMap::new();
        input.insert(1, String::from("A"));
        input.insert(2, String::from("A"));
        input.insert(3, String::from("A"));
        input.insert(4, String::from("B"));
        input.insert(5, String::from("B"));
        input.insert(6, String::from("C"));

        let output = Handle::dedupe(&input);
        let expected_deduped_ids: HashSet<i32> = output.values().copied().collect();
        assert_eq!(expected_deduped_ids.len(), 3);
    }
}
