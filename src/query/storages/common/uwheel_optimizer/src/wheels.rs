// Copyright 2021 Datafuse Labs
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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use uwheel::aggregator::all::AllAggregator;
use uwheel::aggregator::avg::F64AvgAggregator;
use uwheel::aggregator::max::F64MaxAggregator;
use uwheel::aggregator::min::F64MinAggregator;
use uwheel::aggregator::min_max::F64MinMaxAggregator;
use uwheel::aggregator::sum::F64SumAggregator;
use uwheel::aggregator::sum::U32SumAggregator;
use uwheel::wheels::read::ReaderWheel;
use uwheel::Aggregator;

type WheelMap<A> = Arc<Mutex<HashMap<String, ReaderWheel<A>>>>;

#[derive(Clone)]
pub struct BuiltInWheels {
    /// A COUNT(*) wheel over the underlying table data and time column
    pub count: ReaderWheel<U32SumAggregator>,
    /// Min/Max pruning wheels for a specific column
    pub min_max: WheelMap<F64MinMaxAggregator>,
    /// SUM Aggregation Wheel Indices
    pub sum: WheelMap<F64SumAggregator>,
    /// AVG Aggregation Wheel Indices
    pub avg: WheelMap<F64AvgAggregator>,
    /// MAX Aggregation Wheel Indices
    pub max: WheelMap<F64MaxAggregator>,
    /// MIN Aggregation Wheel Indices
    pub min: WheelMap<F64MinAggregator>,
    /// ALL (SUM, AVG, MAX, MIN, COUNT) Aggregation Wheel Indices
    pub all: WheelMap<AllAggregator>,
}
impl BuiltInWheels {
    pub fn new(
        count: ReaderWheel<U32SumAggregator>,
        min_max_wheels: WheelMap<F64MinMaxAggregator>,
    ) -> Self {
        Self {
            count,
            min_max: min_max_wheels,
            sum: Default::default(),
            avg: Default::default(),
            min: Default::default(),
            max: Default::default(),
            all: Default::default(),
        }
    }
    /// Returns the total number of bytes used by all wheel indices
    pub fn index_usage_bytes(&self) -> usize {
        let mut bytes = 0;

        fn wheel_bytes<A: Aggregator>(wheels: &WheelMap<A>) -> usize {
            wheels
                .lock()
                .unwrap()
                .iter()
                .map(|(_, wheel)| wheel.as_ref().size_bytes())
                .sum::<usize>()
        }

        bytes += self.count.as_ref().size_bytes();
        bytes += wheel_bytes(&self.min_max);
        bytes += wheel_bytes(&self.avg);
        bytes += wheel_bytes(&self.sum);
        bytes += wheel_bytes(&self.min);
        bytes += wheel_bytes(&self.max);
        bytes += wheel_bytes(&self.all);

        bytes
    }
}
