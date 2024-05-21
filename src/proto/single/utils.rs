use rand::{thread_rng, Rng};
use serde::{de::Deserializer, Deserialize, Serializer};
use std::time::Duration;

const JOB_ID_LENGTH: usize = 16;
const WORKER_ID_LENGTH: usize = 32;

fn gen_random_id(length: usize) -> String {
    thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .map(char::from)
        .take(length)
        .collect()
}

pub(crate) fn gen_random_jid() -> String {
    gen_random_id(JOB_ID_LENGTH)
}

pub(crate) fn gen_random_wid() -> String {
    gen_random_id(WORKER_ID_LENGTH)
}

pub(crate) fn ser_duration<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let secs = value.as_secs();
    serializer.serialize_u64(secs)
}

pub(crate) fn deser_duration<'de, D>(value: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let secs = u64::deserialize(value)?;
    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_id_of_known_size_generated() {
        let id1 = gen_random_id(WORKER_ID_LENGTH);
        let id2 = gen_random_id(WORKER_ID_LENGTH);
        assert_ne!(id1, id2);
        assert_eq!(id1.len(), WORKER_ID_LENGTH);
        assert_eq!(id2.len(), WORKER_ID_LENGTH);
    }

    #[test]
    fn test_ids_are_unique() {
        let mut ids = HashSet::new();

        ids.insert("IYKOxEfLcwcgKaRa".to_string());
        ids.insert("IYKOxEfLcwcgKaRa".to_string());
        assert_ne!(ids.len(), 2);

        ids.clear();

        for _ in 0..1_000_000 {
            let id = gen_random_id(JOB_ID_LENGTH);
            ids.insert(id);
        }
        assert_eq!(ids.len(), 1_000_000);
    }

    #[test]
    fn test_ser_deser_duration() {
        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
        struct FaktoryServer {
            #[serde(deserialize_with = "deser_duration")]
            #[serde(serialize_with = "ser_duration")]
            uptime: Duration,
        }

        let server = FaktoryServer {
            uptime: Duration::from_secs(2024),
        };

        let serialized = serde_json::to_string(&server).expect("serialized ok");
        let deserialized = serde_json::from_str(&serialized).expect("deserialized ok");
        assert_eq!(server, deserialized);
    }
}
