pub mod raft {
    pub mod v1 {
        tonic::include_proto!("engine.raft.v1");
    }
}

pub mod jni {
    pub mod v1 {
        tonic::include_proto!("engine.jni.v1");
    }
}
