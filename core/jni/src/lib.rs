use std::sync::{mpsc, Mutex};

use engine::config::RaftConfig;
use engine::node::{ProposeReq, ProposeResult, SnapLoadReq, SnapSaveReq};
use engine::peer::{connectpeers, PeerRpc, peerservice};
use engine::wal::{SnapshotStorage, StateStorage};
use jni::objects::{JByteBuffer, JClass, JObject, JObjectArray, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use tokio::sync::{mpsc as tmpsc, oneshot};
use tonic::transport::Server;

struct E { index: u64, term: u64, data: Vec<u8> }

struct S { rt: tokio::runtime::Runtime, ptx: tmpsc::UnboundedSender<ProposeReq>, stx: tmpsc::UnboundedSender<SnapSaveReq>, ltx: tmpsc::UnboundedSender<SnapLoadReq>, crx: Mutex<mpsc::Receiver<E>> }

macro_rules! s {
    ($ptr:expr) => { unsafe { &*($ptr as *const S) } };
}

#[export_name = "Java_engine_client_RaftLib_init"]
pub extern "system" fn init<'local>(
    mut env: JNIEnv<'local>, _cls: JClass<'local>,
    id: jlong, dir: JObject<'local>, peers: JObject<'local>, port: jlong,
) -> jlong {
    let id = id as u64;
    let dir: String = { let j = JString::from(dir); env.get_string(&j).unwrap().into() };
    let port = port as u16;

    let arr = JObjectArray::from(peers);
    let n = env.get_array_length(&arr).unwrap() as usize;
    let mut addrs = Vec::with_capacity(n);
    for i in 0..n {
        let o = env.get_object_array_element(&arr, i as i32).unwrap();
        addrs.push(env.get_string(&JString::from(o)).unwrap().into());
    }

    let cfg = RaftConfig { id, peeraddrs: addrs.clone(), datadir: dir.clone() };
    let rt = match tokio::runtime::Runtime::new() { Ok(r) => r, Err(_) => return 0, };
    let state = StateStorage::new(&dir);
    let snapshot = SnapshotStorage::new(&dir);
    let (peertx, peerrx) = tmpsc::unbounded_channel::<PeerRpc>();
    let (ct, cr) = mpsc::channel::<E>();

    let cb: engine::node::CommittedFn = Box::new(move |i, t, d: &[u8]| { ct.send(E { index: i, term: t, data: d.to_vec() }).ok(); });

    let pcs = rt.block_on(connectpeers(&addrs));
    let rn = engine::node::RaftNode::start(cfg, state, snapshot, pcs, peerrx, cb);

    let svc = peerservice(peertx);
    let addr = format!("0.0.0.0:{}", port);
    rt.spawn(async move { if let Ok(a) = addr.parse() { Server::builder().add_service(svc).serve(a).await.ok(); } });

    Box::into_raw(Box::new(S { rt, ptx: rn.proposetx, stx: rn.snapsavetx, ltx: rn.snaploadtx, crx: Mutex::new(cr) })) as jlong
}

#[export_name = "Java_engine_client_RaftLib_propose"]
pub extern "system" fn propose<'local>(env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong, buf: JObject<'local>) -> jlong {
    let b = JByteBuffer::from(buf);
    let addr = env.get_direct_buffer_address(&b).unwrap();
    let cap = env.get_direct_buffer_capacity(&b).unwrap();
    let data = unsafe { std::slice::from_raw_parts(addr, cap).to_vec() };
    let (tx, rx) = oneshot::channel();
    let s = s!(ptr);
    s.ptx.send(ProposeReq { data, respond: tx }).ok();
    let res = s.rt.block_on(async { rx.await.unwrap_or(ProposeResult { status: engine::status::Status::internal("closed"), index: 0, term: 0 }) });
    ((res.status.code() as u64) << 48 | (res.index & 0x0000_FFFF_FFFF_FFFF)) as jlong
}

#[export_name = "Java_engine_client_RaftLib_recv"]
pub extern "system" fn recv<'local>(env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong, buf: JObject<'local>) -> jint {
    let e = match s!(ptr).crx.lock().unwrap().recv() { Ok(v) => v, Err(_) => return -1 };
    let b = JByteBuffer::from(buf);
    let addr = env.get_direct_buffer_address(&b).unwrap();
    let cap = env.get_direct_buffer_capacity(&b).unwrap();
    let n = 16 + e.data.len();
    if cap < n { return -2 }
    let dst = unsafe { std::slice::from_raw_parts_mut(addr, cap) };
    dst[0..8].copy_from_slice(&e.index.to_le_bytes());
    dst[8..16].copy_from_slice(&e.term.to_le_bytes());
    dst[16..n].copy_from_slice(&e.data);
    n as jint
}

#[export_name = "Java_engine_client_RaftLib_snap"]
pub extern "system" fn snap<'local>(env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong, index: jlong, buf: JObject<'local>) {
    let b = JByteBuffer::from(buf);
    let addr = env.get_direct_buffer_address(&b).unwrap();
    let cap = env.get_direct_buffer_capacity(&b).unwrap();
    s!(ptr).stx.send(SnapSaveReq { index: index as u64, data: unsafe { std::slice::from_raw_parts(addr, cap).to_vec() } }).ok();
}

#[export_name = "Java_engine_client_RaftLib_load"]
pub extern "system" fn load<'local>(env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong, buf: JObject<'local>) -> jint {
    let (tx, rx) = oneshot::channel();
    let s = s!(ptr);
    s.ltx.send(tx).ok();
    let snap = s.rt.block_on(async { rx.await.unwrap_or(None) });
    match snap {
        Some(d) => {
            let b = JByteBuffer::from(buf);
            let addr = env.get_direct_buffer_address(&b).unwrap();
            let cap = env.get_direct_buffer_capacity(&b).unwrap();
            if cap < d.data.len() { return -2 }
            let dst = unsafe { std::slice::from_raw_parts_mut(addr, cap) };
            dst[..d.data.len()].copy_from_slice(&d.data);
            d.data.len() as jint
        }
        None => -1,
    }
}

#[export_name = "Java_engine_client_RaftLib_stop"]
pub extern "system" fn stop<'local>(_env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong) {
    if ptr == 0 { return; }
    unsafe { drop(Box::from_raw(ptr as *mut S)); }
}
