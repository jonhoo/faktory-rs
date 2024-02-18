use chrono::{DateTime, Utc};
use serde::{
    de::{Deserializer, IntoDeserializer},
    Deserialize,
};

// Used to parse responses from Faktory where a datetime field is set to an empty string, e.g:
// '{"jid":"f6APFzrS2RZi9eaA","state":"unknown","updated_at":""}'
pub(crate) fn parse_datetime<'de, D>(value: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<String>::deserialize(value)?.as_deref() {
        Some("") | None => Ok(None),
        Some(non_empty) => DateTime::deserialize(non_empty.into_deserializer()).map(Some),
    }
}
