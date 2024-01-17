use rand::{thread_rng, Rng};

const JOB_ID_LENGTH: usize = 16;
const WORKER_ID_LENGTH: usize = 32;

fn gen_random_id(length: usize) -> String {
    thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .map(char::from)
        .take(length)
        .collect()
}

pub fn gen_random_jid() -> String {
    gen_random_id(JOB_ID_LENGTH)
}

pub fn gen_random_wid() -> String {
    gen_random_id(WORKER_ID_LENGTH)
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;

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
}
