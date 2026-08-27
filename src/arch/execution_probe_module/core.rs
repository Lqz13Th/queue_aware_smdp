use std::sync::Arc;

use extrema_infra::prelude::*;

use super::base::ExecutionProbe;

impl Strategy for ExecutionProbe {
    async fn initialize(&mut self) {
        self.initialize_module().await;
    }

    fn strategy_name(&self) -> &'static str {
        "QueueAwareSmdpExecutionProbe"
    }
}

impl CommandEmitter for ExecutionProbe {
    fn command_init(&mut self, registry: Arc<CommandRegistry>) {
        self.initialize_command_registry(registry);
    }

    fn command_registry(&self) -> Arc<CommandRegistry> {
        self.current_command_registry()
    }
}

impl EventHandler for ExecutionProbe {
    async fn on_ws_event(&mut self, msg: InfraMsg<WsTaskInfo>) {
        self.handle_ws_event(msg).await;
    }

    async fn on_ws_other(&mut self, msg: InfraMsg<Vec<WsOtherMessage>>) {
        self.handle_ws_other(msg).await;
    }

    async fn on_schedule(&mut self, msg: InfraMsg<AltScheduleEvent>) {
        self.handle_schedule(msg).await;
    }
}
