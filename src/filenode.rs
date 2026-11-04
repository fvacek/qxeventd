//! Operations of the SHV file node, see https://silicon-heaven.github.io/shv-doc/rpcmethods/file.html
//!
//! A regular resizable file is provided. A file which does not exist yet is presented as an
//! empty one, it is created by the first write.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{anyhow, bail};
use shvclient::clientnode::{MethodResolver, RequestHandlerResult, err_unresolved_request};
use shvproto::{DateTime, RpcValue};
use shvproto::rpcvalue::IMap;
use shvrpc::metamethod::{AccessLevel, Flags, MetaMethod};
use shvrpc::RpcMessage;
use sha1::{Digest, Sha1};

use crate::{anyhow_to_rpc_error, resolve_error};

pub const METH_STAT: &str = "stat";
pub const METH_SIZE: &str = "size";
pub const METH_CRC: &str = "crc";
pub const METH_SHA1: &str = "sha1";
pub const METH_READ: &str = "read";
pub const METH_WRITE: &str = "write";
pub const METH_TRUNCATE: &str = "truncate";
pub const METH_APPEND: &str = "append";

pub const STAT_RESULT: &str = "!stat";
pub const SIZE_RESULT: &str = "i(0,)";
pub const RANGE_PARAM: &str = "[i(0,):offset,i(0,)|n:size]|n";
pub const CRC_RESULT: &str = "u(>32)";
pub const SHA1_RESULT: &str = "b(20)";
pub const READ_PARAM: &str = "[i(0,):offset,i(0,):size]";
pub const READ_RESULT: &str = "b";
pub const WRITE_PARAM: &str = "[i(0,):offset,b:data]";
pub const TRUNCATE_PARAM: &str = "i(0,)";
pub const APPEND_PARAM: &str = "b";

pub const USER_ID_GETTER: Flags = Flags::IsGetter.union(Flags::UserIDRequired);
pub const USER_ID_LARGE_RESULT: Flags = Flags::LargeResultHint.union(Flags::UserIDRequired);

pub const META_FILE_STAT: MetaMethod = MetaMethod::new_static(
    METH_STAT, USER_ID_GETTER, AccessLevel::Service, "n", STAT_RESULT, &[], "",
);
pub const META_FILE_SIZE: MetaMethod = MetaMethod::new_static(
    METH_SIZE, USER_ID_GETTER, AccessLevel::Service, "n", SIZE_RESULT, &[], "",
);
pub const META_FILE_CRC: MetaMethod = MetaMethod::new_static(
    METH_CRC, Flags::UserIDRequired, AccessLevel::Service, RANGE_PARAM, CRC_RESULT, &[], "",
);
pub const META_FILE_SHA1: MetaMethod = MetaMethod::new_static(
    METH_SHA1, Flags::UserIDRequired, AccessLevel::Service, RANGE_PARAM, SHA1_RESULT, &[], "",
);
pub const META_FILE_READ: MetaMethod = MetaMethod::new_static(
    METH_READ, USER_ID_LARGE_RESULT, AccessLevel::Service, READ_PARAM, READ_RESULT, &[], "",
);
pub const META_FILE_WRITE: MetaMethod = MetaMethod::new_static(
    METH_WRITE, Flags::UserIDRequired, AccessLevel::Service, WRITE_PARAM, "n", &[], "",
);
pub const META_FILE_TRUNCATE: MetaMethod = MetaMethod::new_static(
    METH_TRUNCATE, Flags::UserIDRequired, AccessLevel::Service, TRUNCATE_PARAM, "n", &[], "",
);
pub const META_FILE_APPEND: MetaMethod = MetaMethod::new_static(
    METH_APPEND, Flags::UserIDRequired, AccessLevel::Service, APPEND_PARAM, "n", &[], "",
);


const STAT_TYPE: i32 = 0;
const STAT_SIZE: i32 = 1;
const STAT_PAGE_SIZE: i32 = 2;
const STAT_MOD_TIME: i32 = 4;
const STAT_MAX_WRITE: i32 = 5;
const STAT_MAX_READ: i32 = 6;

const FILE_TYPE_REGULAR: i64 = 0;
const PAGE_SIZE: u64 = 1024 * 64;
const MAX_READ: u64 = 1024 * 4;
const MAX_WRITE: usize = 1024 * 4;

/// Result of the file node stat method
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStat {
    pub file_type: i64,
    pub size: u64,
    pub page_size: u64,
    /// `None` if the file does not exist yet
    pub mod_time: Option<DateTime>,
    pub max_write: u64,
    pub max_read: u64,
}

impl Default for FileStat {
    /// Stat of an empty regular file with the limits of this file node
    fn default() -> Self {
        Self {
            file_type: FILE_TYPE_REGULAR,
            size: 0,
            page_size: PAGE_SIZE,
            mod_time: None,
            max_write: MAX_WRITE as u64,
            max_read: MAX_READ,
        }
    }
}

impl From<FileStat> for RpcValue {
    fn from(stat: FileStat) -> Self {
        let mut result = IMap::new();
        result.insert(STAT_TYPE, stat.file_type.into());
        result.insert(STAT_SIZE, (stat.size as i64).into());
        result.insert(STAT_PAGE_SIZE, (stat.page_size as i64).into());
        result.insert(STAT_MAX_WRITE, (stat.max_write as i64).into());
        result.insert(STAT_MAX_READ, (stat.max_read as i64).into());
        if let Some(mod_time) = stat.mod_time {
            result.insert(STAT_MOD_TIME, mod_time.into());
        }
        result.into()
    }
}

impl TryFrom<&RpcValue> for FileStat {
    type Error = anyhow::Error;

    fn try_from(value: &RpcValue) -> anyhow::Result<Self> {
        let map = value.as_imap();
        let field = |key: i32, name: &str| {
            let value = map.get(&key).ok_or_else(|| anyhow!("Missing stat {name}"))?;
            i64::try_from(value).map_err(|e| anyhow!("Invalid stat {name}: {e}"))
        };
        let unsigned_field = |key, name| to_unsigned(name, field(key, name)?);
        Ok(Self {
            file_type: field(STAT_TYPE, "type")?,
            size: unsigned_field(STAT_SIZE, "size")?,
            page_size: unsigned_field(STAT_PAGE_SIZE, "pageSize")?,
            mod_time: map.get(&STAT_MOD_TIME)
                .map(|value| DateTime::try_from(value).map_err(|e| anyhow!("Invalid stat modTime: {e}")))
                .transpose()?,
            max_write: unsigned_field(STAT_MAX_WRITE, "maxWrite")?,
            max_read: unsigned_field(STAT_MAX_READ, "maxRead")?,
        })
    }
}

#[derive(Debug)]
pub enum FileOp {
    Stat,
    Size,
    Crc { offset: u64, size: Option<u64> },
    Sha1 { offset: u64, size: Option<u64> },
    Read { offset: u64, size: u64 },
    Write { offset: u64, data: Vec<u8> },
    Truncate { size: u64 },
    Append { data: Vec<u8> },
}

/// Resolves a file node method listed in `methods`, so a read only node just omits the methods
/// modifying the file. `rights` is awaited only for a valid request.
pub async fn resolve_op<F, Fut>(
    m: &MethodResolver,
    rq: &RpcMessage,
    methods: &'static [MetaMethod],
    rights: impl Future<Output = Vec<MetaMethod>>,
    execute: F,
) -> RequestHandlerResult
where
    F: FnOnce(FileOp) -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<RpcValue>> + Send + 'static,
{
    let method = m.method();
    if !methods.iter().any(|mm| mm.name == method) {
        return err_unresolved_request();
    }
    let op = match FileOp::from_request(method, rq.param().unwrap_or_default()) {
        Some(Ok(op)) => op,
        Some(Err(err)) => return resolve_error(m, methods, err),
        None => return err_unresolved_request(),
    };
    m.resolve(rights.await, async move || execute(op).await.map_err(anyhow_to_rpc_error))
}

fn to_unsigned(name: &str, value: i64) -> anyhow::Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("{name} must not be negative, got: {value}"))
}

fn optional_unsigned(name: &str, value: &RpcValue) -> anyhow::Result<Option<u64>> {
    if value.is_null() {
        return Ok(None);
    }
    let value = i64::try_from(value).map_err(|e| anyhow!("Invalid {name}: {e}"))?;
    to_unsigned(name, value).map(Some)
}

fn pair_param<'a>(method: &str, param: &'a RpcValue) -> anyhow::Result<(&'a RpcValue, &'a RpcValue)> {
    match param.as_list().as_slice() {
        [first, second] => Ok((first, second)),
        _ => bail!("Invalid {method} param, list of 2 items expected, got: {}", param.to_cpon()),
    }
}

/// Parses the `[offset, size]|n` param of the checksum methods, `None` size means up to the end
/// of the file.
fn parse_range(method: &str, param: &RpcValue) -> anyhow::Result<(u64, Option<u64>)> {
    if param.is_null() {
        return Ok((0, None));
    }
    let (offset, size) = pair_param(method, param)?;
    let offset = optional_unsigned("offset", offset)?;
    let size = optional_unsigned("size", size)?;
    Ok((offset.unwrap_or_default(), size))
}

impl FileOp {
    /// Returns `None` if `method` is not a file node method.
    pub fn from_request(method: &str, param: &RpcValue) -> Option<anyhow::Result<Self>> {
        Some(match method {
            METH_STAT => Ok(Self::Stat),
            METH_SIZE => Ok(Self::Size),
            METH_CRC => parse_range(METH_CRC, param).map(|(offset, size)| Self::Crc { offset, size }),
            METH_SHA1 => parse_range(METH_SHA1, param).map(|(offset, size)| Self::Sha1 { offset, size }),
            METH_READ => Self::parse_read(param),
            METH_WRITE => Self::parse_write(param),
            METH_TRUNCATE => Self::parse_truncate(param),
            METH_APPEND => Self::parse_append(param),
            _ => return None,
        })
    }

    fn parse_read(param: &RpcValue) -> anyhow::Result<Self> {
        let (offset, size) = pair_param(METH_READ, param)?;
        let offset = i64::try_from(offset).map_err(|e| anyhow!("Invalid read offset: {e}"))?;
        let size = i64::try_from(size).map_err(|e| anyhow!("Invalid read size: {e}"))?;
        Ok(Self::Read { offset: to_unsigned("offset", offset)?, size: to_unsigned("size", size)? })
    }

    fn parse_write(param: &RpcValue) -> anyhow::Result<Self> {
        let (offset, data) = pair_param(METH_WRITE, param)?;
        let offset = i64::try_from(offset).map_err(|e| anyhow!("Invalid write offset: {e}"))?;
        let data = <&[u8]>::try_from(data).map_err(|e| anyhow!("Invalid write data: {e}"))?;
        Ok(Self::Write { offset: to_unsigned("offset", offset)?, data: data.to_vec() })
    }

    fn parse_truncate(param: &RpcValue) -> anyhow::Result<Self> {
        let size = i64::try_from(param).map_err(|e| anyhow!("Invalid truncate param: {e}"))?;
        Ok(Self::Truncate { size: to_unsigned("size", size)? })
    }

    fn parse_append(param: &RpcValue) -> anyhow::Result<Self> {
        let data = <&[u8]>::try_from(param).map_err(|e| anyhow!("Invalid append param: {e}"))?;
        Ok(Self::Append { data: data.to_vec() })
    }

    pub fn modifies_file(&self) -> bool {
        matches!(self, Self::Write { .. } | Self::Truncate { .. } | Self::Append { .. })
    }

    /// Blocking, run it outside of the async executor.
    pub fn execute(self, path: &Path) -> anyhow::Result<RpcValue> {
        match self {
            Self::Stat => stat(path).map(RpcValue::from),
            Self::Size => Ok(RpcValue::from(file_size(path)? as i64)),
            Self::Crc { offset, size } => Ok(RpcValue::from(crc(path, offset, size)?)),
            Self::Sha1 { offset, size } => Ok(RpcValue::from(sha1(path, offset, size)?)),
            Self::Read { offset, size } => Ok(RpcValue::from(read(path, offset, size)?)),
            Self::Write { offset, data } => {
                check_write_size(&data)?;
                let mut file = open_for_write(path, OpenOptions::new().write(true).create(true).truncate(false))?;
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(&data)?;
                Ok(RpcValue::null())
            }
            Self::Truncate { size } => {
                let file = open_for_write(path, OpenOptions::new().write(true).create(true).truncate(false))?;
                file.set_len(size)?;
                Ok(RpcValue::null())
            }
            Self::Append { data } => {
                check_write_size(&data)?;
                let mut file = open_for_write(path, OpenOptions::new().append(true).create(true))?;
                file.write_all(&data)?;
                Ok(RpcValue::null())
            }
        }
    }
}

fn check_write_size(data: &[u8]) -> anyhow::Result<()> {
    if data.len() > MAX_WRITE {
        bail!("Cannot write {} bytes at once, the limit is {MAX_WRITE}", data.len());
    }
    Ok(())
}

fn open_for_write(path: &Path, options: &OpenOptions) -> anyhow::Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(options.open(path)?)
}

fn open_for_read(path: &Path) -> anyhow::Result<Option<File>> {
    match File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn file_size(path: &Path) -> anyhow::Result<u64> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err.into()),
    }
}

fn stat(path: &Path) -> anyhow::Result<FileStat> {
    let mod_time = std::fs::metadata(path).and_then(|metadata| metadata.modified()).ok()
        .map(|mod_time| DateTime::from_datetime(&chrono::DateTime::<chrono::Utc>::from(mod_time)));
    Ok(FileStat { size: file_size(path)?, mod_time, ..FileStat::default() })
}

/// Feeds `update` with the part of the range which is in the file, the range can exceed the file.
fn for_each_chunk(path: &Path, offset: u64, size: Option<u64>, mut update: impl FnMut(&[u8])) -> anyhow::Result<()> {
    if let Some(mut file) = open_for_read(path)? {
        file.seek(SeekFrom::Start(offset))?;
        let mut reader = file.take(size.unwrap_or(u64::MAX));
        let mut buffer = vec![0; PAGE_SIZE as usize];
        loop {
            let len = reader.read(&mut buffer)?;
            if len == 0 {
                break;
            }
            update(&buffer[..len]);
        }
    }
    Ok(())
}

/// CRC-32 of the part of the range which is in the file.
fn crc(path: &Path, offset: u64, size: Option<u64>) -> anyhow::Result<u32> {
    let mut crc = flate2::Crc::new();
    for_each_chunk(path, offset, size, |chunk| crc.update(chunk))?;
    Ok(crc.sum())
}

/// SHA-1 of the part of the range which is in the file.
pub fn sha1(path: &Path, offset: u64, size: Option<u64>) -> anyhow::Result<Vec<u8>> {
    let mut hasher = Sha1::new();
    for_each_chunk(path, offset, size, |chunk| hasher.update(chunk))?;
    Ok(hasher.finalize().to_vec())
}

/// Reads at most `MAX_READ` bytes, data out of the file are not returned.
fn read(path: &Path, offset: u64, size: u64) -> anyhow::Result<Vec<u8>> {
    let mut data = Vec::new();
    if let Some(mut file) = open_for_read(path)? {
        file.seek(SeekFrom::Start(offset))?;
        file.take(size.min(MAX_READ)).read_to_end(&mut data)?;
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(method: &str, param: &str) -> anyhow::Result<FileOp> {
        let param = if param.is_empty() { RpcValue::null() } else { RpcValue::from_cpon(param).unwrap() };
        FileOp::from_request(method, &param).expect("file node method")
    }

    #[test]
    fn stat_conversion() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");

        let stat_missing = stat(&path).unwrap();
        assert_eq!(stat_missing, FileStat::default());
        assert_eq!(FileStat::try_from(&RpcValue::from(stat_missing.clone())).unwrap(), stat_missing);

        std::fs::write(&path, b"123456789").unwrap();
        let stat_existing = stat(&path).unwrap();
        assert_eq!(stat_existing.size, 9);
        assert!(stat_existing.mod_time.is_some());
        assert_eq!(FileStat::try_from(&RpcValue::from(stat_existing.clone())).unwrap(), stat_existing);

        assert!(FileStat::try_from(&RpcValue::null()).is_err());
        assert!(FileStat::try_from(&RpcValue::from_cpon("i{0:0,1:-1,2:1,5:1,6:1}").unwrap()).is_err());
    }

    #[test]
    fn unknown_method_is_not_resolved() {
        assert!(FileOp::from_request("foo", &RpcValue::null()).is_none());
    }

    #[test]
    fn crc_param_forms() {
        assert!(matches!(op(METH_CRC, "").unwrap(), FileOp::Crc { offset: 0, size: None }));
        assert!(matches!(op(METH_CRC, "[1,2]").unwrap(), FileOp::Crc { offset: 1, size: Some(2) }));
        assert!(matches!(op(METH_CRC, "[1,null]").unwrap(), FileOp::Crc { offset: 1, size: None }));
        assert!(matches!(op(METH_CRC, "[null,2]").unwrap(), FileOp::Crc { offset: 0, size: Some(2) }));
        assert!(op(METH_CRC, "[-1,2]").is_err());
        assert!(op(METH_CRC, "\"foo\"").is_err());
    }

    #[test]
    fn sha1_param_forms() {
        assert!(matches!(op(METH_SHA1, "").unwrap(), FileOp::Sha1 { offset: 0, size: None }));
        assert!(matches!(op(METH_SHA1, "[1,2]").unwrap(), FileOp::Sha1 { offset: 1, size: Some(2) }));
        assert!(matches!(op(METH_SHA1, "[1,null]").unwrap(), FileOp::Sha1 { offset: 1, size: None }));
        assert!(matches!(op(METH_SHA1, "[null,2]").unwrap(), FileOp::Sha1 { offset: 0, size: Some(2) }));
        assert!(op(METH_SHA1, "[-1,2]").is_err());
        assert!(op(METH_SHA1, "\"foo\"").is_err());
    }

    #[test]
    fn sha1_out_of_file_range() {
        const SHA1_EMPTY: [u8; 20] = [
            0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55,
            0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
        ];
        const SHA1_DIGITS: [u8; 20] = [
            0xf7, 0xc3, 0xbc, 0x1d, 0x80, 0x8e, 0x04, 0x73, 0x2a, 0xdf,
            0x67, 0x99, 0x65, 0xcc, 0xc3, 0x4c, 0xa7, 0xae, 0x34, 0x41,
        ];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        assert_eq!(sha1(&path, 0, None).unwrap(), SHA1_EMPTY);
        assert!(!path.exists());
        std::fs::write(&path, b"123456789").unwrap();
        assert_eq!(sha1(&path, 0, None).unwrap(), SHA1_DIGITS);
        assert_eq!(sha1(&path, 0, Some(100)).unwrap(), SHA1_DIGITS);
        assert_eq!(sha1(&path, 100, None).unwrap(), SHA1_EMPTY);
    }

    #[test]
    fn invalid_params_are_rejected() {
        for (method, param) in [
            (METH_READ, ""),
            (METH_READ, "[0]"),
            (METH_READ, "[-1,1]"),
            (METH_READ, "[0,-1]"),
            (METH_READ, "[\"a\",1]"),
            (METH_WRITE, "[0,1]"),
            (METH_WRITE, "[-1,b\"a\"]"),
            (METH_TRUNCATE, ""),
            (METH_TRUNCATE, "-1"),
            (METH_APPEND, ""),
            (METH_APPEND, "1"),
        ] {
            assert!(op(method, param).is_err(), "{method} {param}");
        }
    }

    #[test]
    fn missing_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        assert_eq!(file_size(&path).unwrap(), 0);
        assert_eq!(read(&path, 0, 10).unwrap(), b"");
        assert_eq!(crc(&path, 0, None).unwrap(), 0);
        assert!(!path.exists());
    }

    #[test]
    fn read_and_crc_out_of_file_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        std::fs::write(&path, b"123456789").unwrap();
        assert_eq!(read(&path, 7, 10).unwrap(), b"89");
        assert_eq!(read(&path, 100, 10).unwrap(), b"");
        assert_eq!(crc(&path, 0, None).unwrap(), 0xCBF43926);
        assert_eq!(crc(&path, 0, Some(100)).unwrap(), 0xCBF43926);
        assert_eq!(crc(&path, 100, None).unwrap(), 0);
    }

    #[test]
    fn write_is_limited() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        let data = vec![0; MAX_WRITE + 1];
        assert!(FileOp::Write { offset: 0, data: data.clone() }.execute(&path).is_err());
        assert!(FileOp::Append { data }.execute(&path).is_err());
        assert!(!path.exists());
    }

    #[test]
    fn read_is_limited() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        std::fs::File::create(&path).unwrap().set_len(MAX_READ + 1).unwrap();
        assert_eq!(read(&path, 0, MAX_READ + 1).unwrap().len() as u64, MAX_READ);
    }
}
