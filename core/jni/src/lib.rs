use std::sync::{mpsc, Mutex};

use engine::config::RaftConfig;
use engine::node::{NodeStatus, ProposeReq, ProposeResult, SnapLoadReq, SnapSaveReq, StatusReq};
use engine::peer::{connectpeers, PeerRpc, peerservice};
use engine::wal::{SnapshotStorage, StateStorage};
use jni::objects::{JByteBuffer, JClass, JObject, JObjectArray, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use prost::Message;
use proto::jni::v1::{CommittedEntry, ProposeRequest, ProposeResponse, SnapshotLoad, SnapshotSave, NodeStatus as JniNodeStatus};
use tokio::sync::{mpsc as tmpsc, oneshot};
use tonic::transport::Server;

struct JniState {
    rt: tokio::runtime::Runtime,
    ptx: tmpsc::UnboundedSender<ProposeReq>,
    stx: tmpsc::UnboundedSender<SnapSaveReq>,
    ltx: tmpsc::UnboundedSender<SnapLoadReq>,
    statustx: tmpsc::UnboundedSender<StatusReq>,
    crx: Mutex<mpsc::Receiver<CommittedEntry>>,
}

macro_rules! state {
    ($ptr:expr) => { unsafe { &*($ptr as *const JniState) } };
}

fn readbuf<'local>(env: &mut JNIEnv<'local>, buf: JObject<'local>) -> Vec<u8> {
    let b = JByteBuffer::from(buf);
    let addr = env.get_direct_buffer_address(&b).unwrap();
    let cap = env.get_direct_buffer_capacity(&b).unwrap();
    unsafe { std::slice::from_raw_parts(addr, cap).to_vec() }
}

fn writebuf<'local>(env: &mut JNIEnv<'local>, buf: JObject<'local>, data: &[u8]) -> jint {
    let b = JByteBuffer::from(buf);
    let addr = env.get_direct_buffer_address(&b).unwrap();
    let cap = env.get_direct_buffer_capacity(&b).unwrap();
    if cap < data.len() { return -2; }
    let dst = unsafe { std::slice::from_raw_parts_mut(addr, cap) };
    dst[..data.len()].copy_from_slice(data);
    data.len() as jint
}

fn tojlong(status: u32, index: u64) -> jlong {
    ((status as u64) << 48 | (index & 0x0000_FFFF_FFFF_FFFF)) as jlong
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
    let (ct, cr) = mpsc::channel::<CommittedEntry>();

    let cb: engine::node::CommittedFn = Box::new(move |i, t, d: &[u8]| {
        ct.send(CommittedEntry { index: i, term: t, data: d.to_vec() }).ok();
    });

    let pcs = rt.block_on(connectpeers(&addrs));
    let rn = engine::node::RaftNode::start(cfg, state, snapshot, pcs, peerrx, cb);

    let svc = peerservice(peertx);
    let addr = format!("0.0.0.0:{}", port);
    rt.spawn(async move {
        if let Ok(a) = addr.parse() { Server::builder().add_service(svc).serve(a).await.ok(); }
    });

    Box::into_raw(Box::new(JniState {
        rt, ptx: rn.proposetx, stx: rn.snapsavetx, ltx: rn.snaploadtx,
        statustx: rn.statustx, crx: Mutex::new(cr),
    })) as jlong
}

#[export_name = "Java_engine_client_RaftLib_propose"]
pub extern "system" fn propose<'local>(mut env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong, buf: JObject<'local>) -> jlong {
    let req = ProposeRequest::decode(readbuf(&mut env, buf).as_slice()).unwrap();
    let (tx, rx) = oneshot::channel();
    let s = state!(ptr);
    s.ptx.send(ProposeReq { data: req.data, respond: tx }).ok();
    let res = s.rt.block_on(async { rx.await.unwrap_or(ProposeResult { status: engine::status::Status::internal("closed"), index: 0, term: 0 }) });
    tojlong(res.status.code() as u32, res.index)
}

#[export_name = "Java_engine_client_RaftLib_recv"]
pub extern "system" fn recv<'local>(mut env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong, buf: JObject<'local>) -> jint {
    let e = match state!(ptr).crx.lock().unwrap().recv() { Ok(v) => v, Err(_) => return -1 };
    let mut out = Vec::with_capacity(e.encoded_len());
    e.encode(&mut out).unwrap();
    writebuf(&mut env, buf, &out)
}

#[export_name = "Java_engine_client_RaftLib_snap"]
pub extern "system" fn snap<'local>(mut env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong, buf: JObject<'local>) {
    let req = SnapshotSave::decode(readbuf(&mut env, buf).as_slice()).unwrap();
    state!(ptr).stx.send(SnapSaveReq { index: req.index, data: req.data }).ok();
}

#[export_name = "Java_engine_client_RaftLib_load"]
pub extern "system" fn load<'local>(mut env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong, buf: JObject<'local>) -> jint {
    let (tx, rx) = oneshot::channel();
    let s = state!(ptr);
    s.ltx.send(tx).ok();
    let snap = s.rt.block_on(async { rx.await.unwrap_or(None) });
    match snap {
        Some(d) => {
            let msg = SnapshotLoad { found: true, lastidx: d.lastindex, lastterm: d.lastterm, data: d.data.clone() };
            let mut out = Vec::with_capacity(msg.encoded_len());
            msg.encode(&mut out).unwrap();
            let b = JByteBuffer::from(buf);
            let addr = env.get_direct_buffer_address(&b).unwrap();
            let cap = env.get_direct_buffer_capacity(&b).unwrap();
            if cap < out.len() { return -2; }
            let dst = unsafe { std::slice::from_raw_parts_mut(addr, cap) };
            dst[..out.len()].copy_from_slice(&out);
            out.len() as jint
        }
        None => {
            let msg = SnapshotLoad { found: false, lastidx: 0, lastterm: 0, data: vec![] };
            let mut out = Vec::with_capacity(msg.encoded_len());
            msg.encode(&mut out).unwrap();
            let b = JByteBuffer::from(buf);
            let addr = env.get_direct_buffer_address(&b).unwrap();
            let cap = env.get_direct_buffer_capacity(&b).unwrap();
            if cap < out.len() { return -2; }
            let dst = unsafe { std::slice::from_raw_parts_mut(addr, cap) };
            dst[..out.len()].copy_from_slice(&out);
            out.len() as jint
        }
    }
}

#[export_name = "Java_engine_client_RaftLib_status"]
pub extern "system" fn status<'local>(mut env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong, buf: JObject<'local>) -> jint {
    let (tx, rx) = oneshot::channel();
    let s = state!(ptr);
    s.statustx.send(tx).ok();
    let st: NodeStatus = s.rt.block_on(async { rx.await.unwrap_or(NodeStatus { isleader: false, term: 0, commitidx: 0, applyidx: 0, leaderid: 0 }) });
    let msg = JniNodeStatus { isleader: st.isleader, term: st.term, commitidx: st.commitidx, applyidx: st.applyidx, leaderid: st.leaderid };
    let mut out = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut out).unwrap();
    writebuf(&mut env, buf, &out)
}

#[export_name = "Java_engine_client_RaftLib_stop"]
pub extern "system" fn stop<'local>(_env: JNIEnv<'local>, _cls: JClass<'local>, ptr: jlong) {
    if ptr == 0 { return; }
    unsafe { drop(Box::from_raw(ptr as *mut JniState)); }
}
