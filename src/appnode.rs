
use std::sync::OnceLock;
use anyhow::anyhow;
use async_trait::async_trait;
use shvclient::ClientCommandSender;
use shvclient::appnodes::{DOT_APP_METHODS, DotAppNode};
use shvclient::clientnode::StaticNode;
use shvrpc::metamethod::{AccessLevel, Flag, MetaMethod};
use shvrpc::{RpcMessageMetaTags, RpcMessage, rpcmessage::RpcError};
use shvproto::RpcValue;

use crate::{anyhow_to_rpc_error, global_config};
use crate::state::SharedAppState;

pub struct AppNode {
    app_state: SharedAppState,
    dot_app_node: DotAppNode,
}

impl AppNode {
    pub fn new(app_name: impl Into<String>, app_state: SharedAppState) -> Self {
        Self {
            app_state,
            dot_app_node: DotAppNode::new(app_name),
        }
    }
}

static METHODS: OnceLock<Vec<MetaMethod>> = OnceLock::new();

fn get_methods() -> &'static [MetaMethod] {
    METHODS.get_or_init(|| {
        let mut mm = Vec::from_iter(DOT_APP_METHODS.iter().cloned());
        mm.extend_from_slice(APP_METHODS);
        mm
    })
}

const METH_CONFIG: &str = "config";
const METH_QUIT: &str = "quit";

pub const APP_METHODS: &[MetaMethod] = &[
    MetaMethod::new_static(
        METH_CONFIG, Flag::None as u32, AccessLevel::Read, "", "", &[], "",
    ),
    MetaMethod::new_static(
        METH_QUIT, Flag::None as u32, AccessLevel::Write, "", "", &[], "",
    ),
];

#[async_trait]
impl StaticNode for AppNode {
    fn methods(&self) -> &'static [MetaMethod] {
        get_methods()
    }

    async fn process_request(&self, request: RpcMessage, client_command_sender: ClientCommandSender) -> Option<Result<RpcValue, RpcError>> {
        match request.method() {
            Some(METH_CONFIG) => {
                match serde_yaml::to_string(global_config()) {
                    Ok(s) => Some(Ok(shvproto::RpcValue::from(s))),
                    Err(e) => Some(Err(anyhow_to_rpc_error(anyhow!("Failed to serialize configuration: {}", e)))),
                }
            }
            Some(METH_QUIT) => {
                log::info!("Exit method called, initiating graceful shutdown");
                match self.app_state.write().await.quit_app(client_command_sender).await {
                    Ok(_) => Some(Ok(shvproto::RpcValue::from(()))),
                    Err(e) => Some(Err(anyhow_to_rpc_error(e))),
                }
            }
            _ => self.dot_app_node.process_request(request, client_command_sender).await,
        }
    }
}
