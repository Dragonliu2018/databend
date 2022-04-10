// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Processor;

pub struct TransformCompact<T: Compactor + Send + 'static> {
    state: ProcessorState,
    compactor: T,
}

pub trait Compactor {
    fn name() -> &'static str;
    fn compact(&self, blocks: &Vec<DataBlock>) -> Result<Vec<DataBlock>>;
}

impl<T: Compactor + Send + 'static> TransformCompact<T> {
    pub fn try_create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        compactor: T,
    ) -> Result<ProcessorPtr> {
        let state = ProcessorState::Consume(ConsumeState {
            input_port,
            output_port,
            input_data_blocks: vec![],
        });

        Ok(ProcessorPtr::create(Box::new(Self { state, compactor })))
    }

    #[inline(always)]
    fn consume_event(&mut self) -> Result<Event> {
        if let ProcessorState::Consume(state) = &mut self.state {
            if state.input_port.is_finished() {
                let mut temp_state = ProcessorState::Finished;
                std::mem::swap(&mut self.state, &mut temp_state);
                temp_state = temp_state.convert_to_compacting_state()?;
                std::mem::swap(&mut self.state, &mut temp_state);
                return Ok(Event::Sync);
            }

            if state.input_port.has_data() {
                state
                    .input_data_blocks
                    .push(state.input_port.pull_data().unwrap()?);
            }

            state.input_port.set_need_data();
            return Ok(Event::NeedData);
        }

        Err(ErrorCode::LogicalError("It's a bug"))
    }
}

#[async_trait::async_trait]
impl<T: Compactor + Send + 'static> Processor for TransformCompact<T> {
    fn name(&self) -> &'static str {
        T::name()
    }

    fn event(&mut self) -> Result<Event> {
        match &mut self.state {
            ProcessorState::Finished => Ok(Event::Finished),
            ProcessorState::Consume(_) => self.consume_event(),
            ProcessorState::Compacting(_) => Err(ErrorCode::LogicalError("It's a bug.")),
            ProcessorState::Compacted(state) => {
                if state.output_port.is_finished() {
                    state.input_port.finish();
                    return Ok(Event::Finished);
                }

                if !state.output_port.can_push() {
                    return Ok(Event::NeedConsume);
                }

                match state.compacted_blocks.pop_front() {
                    None => {
                        state.output_port.finish();
                        Ok(Event::Finished)
                    }
                    Some(data) => {
                        state.output_port.push_data(Ok(data));
                        Ok(Event::NeedConsume)
                    }
                }
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        if let ProcessorState::Compacting(state) = &self.state {
            let compacted_blocks = self.compactor.compact(&state.blocks)?;

            let mut temp_state = ProcessorState::Finished;
            std::mem::swap(&mut self.state, &mut temp_state);
            temp_state = temp_state.convert_to_compacted_state(compacted_blocks)?;
            std::mem::swap(&mut self.state, &mut temp_state);
            debug_assert!(matches!(temp_state, ProcessorState::Finished));
            return Ok(());
        }

        Err(ErrorCode::LogicalError("State invalid. it's a bug."))
    }
}

enum ProcessorState {
    Consume(ConsumeState),
    Compacting(CompactingState),
    Compacted(CompactedState),
    Finished,
}

pub struct CompactedState {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    compacted_blocks: VecDeque<DataBlock>,
}

pub struct ConsumeState {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    input_data_blocks: Vec<DataBlock>,
}

pub struct CompactingState {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    blocks: Vec<DataBlock>,
}

impl ProcessorState {
    #[inline(always)]
    fn convert_to_compacting_state(self) -> Result<Self> {
        match self {
            ProcessorState::Consume(state) => Ok(ProcessorState::Compacting(CompactingState {
                input_port: state.input_port,
                output_port: state.output_port,
                blocks: state.input_data_blocks,
            })),
            _ => Err(ErrorCode::LogicalError(
                "State invalid, must be consume state",
            )),
        }
    }

    #[inline(always)]
    fn convert_to_compacted_state(self, compacted_blocks: Vec<DataBlock>) -> Result<Self> {
        match self {
            ProcessorState::Compacting(state) => {
                let compacted_blocks = VecDeque::from(compacted_blocks);
                Ok(ProcessorState::Compacted(CompactedState {
                    input_port: state.input_port,
                    output_port: state.output_port,
                    compacted_blocks,
                }))
            }
            _ => Err(ErrorCode::LogicalError(
                "State invalid, must be compacted state",
            )),
        }
    }
}
