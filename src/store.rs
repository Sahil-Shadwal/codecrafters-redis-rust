use std::collections::HashMap;
use std::env::args;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

use std::fs::File;
use std::io::{BufReader, Read};

#[derive(Debug)]
pub struct Config {
    dir: Option<String>,
    dbfilename: Option<String>,
}

#[derive(Clone)]
struct ExpiringValue {
    value: String,
    expires_at: Option<SystemTime>,
}

pub struct Database {
    config: Config,
    db: RwLock<HashMap<String, ExpiringValue>>,
}

impl Config {
    pub fn new() -> Self {
        Config {
            dir: None,
            dbfilename: None,
        }
    }

    pub fn from_args(&mut self) {
        let args: Vec<String> = args().collect();
        let mut iter = args.iter();
        while let Some(arg) = iter.next() {
            match arg.to_lowercase().as_str() {
                "--dir" => {
                    self.dir = iter.next().map(|s| s.to_owned());
                }
                "--dbfilename" => {
                    self.dbfilename = iter.next().map(|s| s.to_owned());
                }
                _ => {}
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        match key.to_lowercase().as_str() {
            "dir" => self.dir.clone(),
            "dbfilename" => self.dbfilename.clone(),
            _ => None,
        }
    }

    pub fn get_file_path(&self) -> Option<String> {
        match (&self.dir, &self.dbfilename) {
            (Some(dir), Some(dbfilename)) => Some(format!("{}/{}", dir, dbfilename)),
            _ => None,
        }
    }
}
impl Database {
    pub fn new() -> Self {
        let mut config = Config::new();
        config.from_args();
        let db = match config.get_file_path() {
            Some(file_path) => {
                if let Some(file) = File::open(file_path).ok() {
                    println!("reading from file");
                    serialize(file)
                } else {
                    HashMap::new()
                }
            }
            None => HashMap::new(),
        };

        Database {
            config,
            db: RwLock::new(db),
        }
    }

    pub async fn set(&self, key: &str, value: &str) {
        let value = ExpiringValue {
            value: value.to_owned(),
            expires_at: None,
        };
        let mut db = self.db.write().await;
        db.insert(key.to_owned(), value);
    }

    pub async fn set_with_expire(&self, key: &str, value: &str, expiry_in_ms: u64) {
        let now = SystemTime::now();
        let duration = Duration::from_millis(expiry_in_ms);
        let value = ExpiringValue {
            value: value.to_owned(),
            expires_at: Some(now + duration),
        };
        let mut db = self.db.write().await;
        db.insert(key.to_owned(), value);
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let now = SystemTime::now();

        let value = {
            let db = self.db.read().await;
            db.get(key).cloned()
        };
        match value {
            Some(v) => match v.expires_at {
                Some(expires_at) if expires_at < now => {
                    println!("now: {:?}, expires_at: {:?}", now, expires_at);
                    let mut db = self.db.write().await;
                    db.remove(key);
                    None
                }
                _ => Some(v.value),
            },
            None => None,
        }
    }

    pub async fn keys(&self, pattern: &str) -> Vec<String> {
        let now = SystemTime::now();
        let mut expired_keys = Vec::new();
        let mut valid_keys = Vec::new();

        {
            let db = self.db.read().await;
            for (key, value) in db.iter() {
                match value.expires_at {
                    Some(expires_at) if expires_at < now => {
                        expired_keys.push(key.to_owned());
                    }
                    _ => {
                        valid_keys.push(key.to_owned());
                    }
                }
            }
        }

        {
            let mut db = self.db.write().await;
            for key in expired_keys {
                db.remove(&key);
            }
        }

        valid_keys
    }

    pub async fn config_get(&self, key: &str) -> Option<String> {
        self.config.get(key)
    }
}

fn length_encode(buf: &[u8]) -> Option<(usize, usize)> {
    let mask = 3u8 << 6; // 1100 0000
    let num = match buf[0] & mask {
        0 => (u32::from_be_bytes([0, 0, 0, buf[0]]), 1),
        64u8 => (u32::from_be_bytes([0, 0, buf[0] & (64u8 - 1), buf[1]]), 2),
        128u8 => (u32::from_be_bytes(buf[1..5].try_into().unwrap()), 5),
        192u8 => return None,
        _ => unreachable!(),
    };
    let num = (num.0 as usize, num.1);
    Some(num)
}

fn serialize_kv(buf: &[u8]) -> Option<(String, ExpiringValue, usize)> {
    let is_expired = buf[0] == 0xfc;
    let expires_at = if is_expired {
        let expires_at = u64::from_le_bytes(buf[1..9].try_into().unwrap());
        Some(UNIX_EPOCH + Duration::from_millis(expires_at as u64))
    } else {
        None
    };
    let mut pos = if is_expired { 10 } else { 1 };

    let (key_len, offset) = length_encode(&buf[pos..]).unwrap();
    pos += offset;
    let key = String::from_utf8(buf[pos..pos + key_len].to_vec()).unwrap();
    pos += key_len;

    let (value_len, offset) = length_encode(&buf[pos..]).unwrap();
    pos += offset;
    let value = String::from_utf8(buf[pos..pos + value_len].to_vec()).unwrap();

    let value = ExpiringValue {
        value,
        expires_at: expires_at,
    };
    Some((key, value, pos + value_len))
}

fn serialize(file: File) -> HashMap<String, ExpiringValue> {
    let now = SystemTime::now();
    println!("now: {:?}", now);
    let mut reader = BufReader::new(file);
    let mut buf = [0u8; 1024];
    let bytes_read = reader.read(&mut buf).unwrap();

    let fb_pos = buf.iter().position(|&b| b == 0xfb).unwrap();
    let mut pos = fb_pos + 1;
    let (hashtable_size, offset) = length_encode(&buf[pos..]).unwrap();
    pos += offset;
    let (exprie_hashtable_size, offset) = length_encode(&buf[pos..]).unwrap();
    pos += offset;

    let mut db = HashMap::new();
    for _ in 0..hashtable_size {
        let (key, value, offset) = serialize_kv(&buf[pos..]).unwrap();
        match value.expires_at {
            Some(expires_at) if expires_at < now => {
                println!("key: {}, expires_at: {:?}", key, expires_at);
            }
            _ => {
                println!("key: {}, expires_at: {:?}", key, value.expires_at);
                db.insert(key, value);
            }
        }
        pos += offset;
    }

    db
}