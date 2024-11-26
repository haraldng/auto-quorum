use std::{collections::HashMap, fs::File, io::Write};

use chrono::Utc;
use csv::Writer;
use metronome::common::{kv::CommandId, messages::MetronomeConfigInfo};
use serde::Serialize;
use tokio::time::Instant;

use crate::configs::{ClientConfig, EndConditionConfig, RequestModeConfig};

pub enum ClientData {
    Full(FullDataCollector),
    MovingAverage(MovingAverageCollector),
}

impl ClientData {
    pub fn new(config: &ClientConfig) -> Self {
        match config.summary_only {
            true => ClientData::MovingAverage(MovingAverageCollector::new(config)),
            false => ClientData::Full(FullDataCollector::new(config)),
        }
    }

    pub fn experiment_start(&mut self) {
        match self {
            ClientData::Full(collector) => collector.experiment_start(),
            ClientData::MovingAverage(collector) => collector.experiment_start(),
        }
    }

    pub fn experiment_end(&mut self) {
        match self {
            ClientData::Full(collector) => collector.experiment_end(),
            ClientData::MovingAverage(collector) => collector.experiment_end(),
        }
    }

    pub fn new_request(&mut self, request_id: CommandId) {
        match self {
            ClientData::Full(collector) => collector.new_request(request_id),
            ClientData::MovingAverage(collector) => collector.new_request(request_id),
        }
    }

    pub fn new_response(&mut self, response_id: CommandId) {
        match self {
            ClientData::Full(collector) => collector.new_response(response_id),
            ClientData::MovingAverage(collector) => collector.new_response(response_id),
        }
    }

    pub fn new_batch_request(&mut self, start_id: CommandId, num_requests: usize) {
        match self {
            ClientData::Full(collector) => collector.new_batch_request(start_id, num_requests),
            ClientData::MovingAverage(collector) => {
                collector.new_batch_request(start_id, num_requests)
            }
        }
    }

    pub fn save_summary(
        &self,
        client_config: ClientConfig,
        server_info: MetronomeConfigInfo,
    ) -> Result<(), std::io::Error> {
        match self {
            ClientData::Full(collector) => collector.save_summary(client_config, server_info),
            ClientData::MovingAverage(collector) => {
                collector.save_summary(client_config, server_info)
            }
        }
    }

    pub fn to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        match self {
            ClientData::Full(collector) => collector.to_csv(file_path),
            ClientData::MovingAverage(collector) => collector.to_csv(file_path),
        }
    }

    pub fn total_responses(&self) -> usize {
        match self {
            ClientData::Full(collector) => collector.total_responses(),
            ClientData::MovingAverage(collector) => collector.total_responses(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ClientSummary {
    client_config: ClientConfig,
    server_info: MetronomeConfigInfo,
    client_start_time: Timestamp,
    throughput: f64,
    total_responses: usize,
    unanswered_requests: usize,
    total_time: f64,
    request_latency_average: f64,
    request_latency_std_dev: f64,
}

pub struct FullDataCollector {
    response_data: Vec<ResponseData>,
    request_count: usize,
    response_count: usize,
    client_start_time: Option<Timestamp>,
    client_end_time: Option<Timestamp>,
}

#[derive(Debug, Serialize)]
pub struct ResponseData {
    command_id: CommandId,
    request_time: Option<Timestamp>,
    response_time: Option<Timestamp>,
}
type Timestamp = i64;
const MS_IN_SEC: usize = 1_000;

impl FullDataCollector {
    fn new(config: &ClientConfig) -> Self {
        // Estimate how many requests will be made so we can appropriately determine starting
        // capacity of request_data vec and avoid allocation latency during experiment
        let estimated_num_requests = match (config.request_mode_config, config.end_condition) {
            (
                RequestModeConfig::OpenLoop(req_interval, interval_batch),
                EndConditionConfig::SecondsPassed(s),
            ) => {
                let expected_requests =
                    (MS_IN_SEC * interval_batch * s as usize) as f64 / req_interval as f64;
                expected_requests.ceil() as usize
            }
            (
                RequestModeConfig::OpenLoop(req_interval, interval_batch),
                EndConditionConfig::ResponsesCollected(n),
            ) => {
                let expected_requests_in_one_sec =
                    ((MS_IN_SEC * interval_batch) as f64 / req_interval as f64).ceil() as usize;
                n + expected_requests_in_one_sec
            }
            (
                RequestModeConfig::ClosedLoop(parallel_requests),
                EndConditionConfig::ResponsesCollected(n),
            ) => n + parallel_requests,
            (
                RequestModeConfig::ClosedLoop(parallel_requests),
                EndConditionConfig::SecondsPassed(s),
            ) => {
                // Worst-case numbers based off data from in-memory runs
                let throughput_wall = 1_000_000;
                let total_wall = throughput_wall * s as usize;
                let estimated_throughput = parallel_requests * 1000;
                let estimated_total = estimated_throughput * s as usize;
                estimated_total.min(total_wall)
            }
        };
        let preallocated_request_data_vec = Vec::with_capacity(estimated_num_requests);
        Self {
            response_data: preallocated_request_data_vec,
            request_count: 0,
            response_count: 0,
            client_start_time: None,
            client_end_time: None,
        }
    }

    fn experiment_start(&mut self) {
        self.client_start_time = Some(Utc::now().timestamp_micros());
    }
    fn experiment_end(&mut self) {
        self.client_end_time = Some(Utc::now().timestamp_micros());
    }

    // A Client can send requests with out-of-order command ids due to parallel requests.
    // Use a vec with placeholder values to fill in the gaps.
    #[inline]
    fn new_request(&mut self, request_id: CommandId) {
        let request_time = Utc::now().timestamp_micros();
        if request_id < self.response_data.len() {
            self.response_data[request_id].request_time = Some(request_time);
        } else {
            for placeholder_id in self.response_data.len()..request_id {
                let placeholder = ResponseData {
                    command_id: placeholder_id,
                    request_time: None,
                    response_time: None,
                };
                self.response_data.push(placeholder);
            }
            self.response_data.push(ResponseData {
                command_id: request_id,
                request_time: Some(request_time),
                response_time: None,
            });
        }
        self.request_count += 1;
    }

    #[inline]
    fn new_response(&mut self, response_id: CommandId) {
        let response_time = Utc::now().timestamp_micros();
        // Placeholders will ensure indexing works
        self.response_data[response_id].response_time = Some(response_time);
        self.response_count += 1;
    }

    #[inline]
    fn new_batch_request(&mut self, start_id: CommandId, num_requests: usize) {
        let request_time = Utc::now().timestamp_micros();
        for request_id in start_id..start_id + num_requests {
            if request_id < self.response_data.len() {
                self.response_data[request_id].request_time = Some(request_time);
            } else {
                for placeholder_id in self.response_data.len()..request_id {
                    let placeholder = ResponseData {
                        command_id: placeholder_id,
                        request_time: None,
                        response_time: None,
                    };
                    self.response_data.push(placeholder);
                }
                self.response_data.push(ResponseData {
                    command_id: request_id,
                    request_time: Some(request_time),
                    response_time: None,
                });
            }
            self.request_count += 1;
        }
    }

    fn save_summary(
        &self,
        client_config: ClientConfig,
        server_info: MetronomeConfigInfo,
    ) -> Result<(), std::io::Error> {
        let summary_filepath = client_config.summary_filename.clone();
        let (request_latency_average, request_latency_std_dev) = self.avg_response_latency();
        let summary = ClientSummary {
            client_config,
            server_info,
            client_start_time: self
                .client_start_time
                .expect("Should create summary after experiment start"),
            throughput: self.throughput(),
            total_responses: self.total_responses(),
            unanswered_requests: self.unanswered_requests(),
            total_time: self.total_time(),
            request_latency_average,
            request_latency_std_dev,
        };
        let json_summary = serde_json::to_string_pretty(&summary)?;
        eprintln!("{json_summary}");
        let mut summary_file = File::create(summary_filepath)?;
        summary_file.write_all(json_summary.as_bytes())?;
        summary_file.flush()?;
        Ok(())
    }

    fn to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        let file = File::create(file_path)?;
        let mut writer = Writer::from_writer(file);
        for data in &self.response_data {
            writer.serialize(data)?;
        }
        writer.flush()?;
        Ok(())
    }

    fn total_responses(&self) -> usize {
        self.response_count
    }

    // The (average, std dev) response latency of all requests in milliseconds
    fn avg_response_latency(&self) -> (f64, f64) {
        let latencies: Vec<i64> = self
            .response_data
            .iter()
            .filter_map(|d| {
                d.response_time.map(|resp_time| {
                    resp_time - d.request_time.expect("Got a response without a request")
                })
            })
            .collect();
        let num_responses = latencies.len();
        let avg_latency = latencies.iter().sum::<Timestamp>() / num_responses as i64;
        let variance = latencies
            .iter()
            .map(|&value| {
                let diff = value - avg_latency;
                diff * diff
            })
            .sum::<Timestamp>() as f64
            / num_responses as f64;
        let std_dev = variance.sqrt();
        (avg_latency as f64 / 1000., std_dev / 1000.)
    }

    // The response throughput in responses/sec
    fn throughput(&self) -> f64 {
        let experiment_duration = self
            .client_end_time
            .expect("Should calculate throughput after experiment end")
            - self
                .client_start_time
                .expect("Should calculate throughput after experiment start");
        let throughput = (self.response_count as f64 / experiment_duration as f64) * 1_000_000.;
        return throughput;
    }

    // The total duration of request time in seconds
    fn total_time(&self) -> f64 {
        let first_request_time = self.response_data[0]
            .request_time
            .expect("First request shouldn't be a placeholder");
        let last_response_time = self
            .response_data
            .iter()
            .rev()
            .filter_map(|d| d.response_time)
            .next()
            .expect("There should be responses");
        (last_response_time - first_request_time) as f64 / 1_000_000.
    }

    fn unanswered_requests(&self) -> usize {
        self.request_count - self.response_count
    }
}

pub struct MovingAverageCollector {
    pending_requests: HashMap<CommandId, Instant>,
    request_count: usize,
    response_count: usize,
    latency_avg: f64,
    latency_m2: f64, // M2 from Welford's online algorithm
    client_start_time: Option<Timestamp>,
    client_end_time: Option<Timestamp>,
    first_request_time: Option<Instant>,
    last_response_time: Option<Instant>,
}

impl MovingAverageCollector {
    fn new(config: &ClientConfig) -> Self {
        let estimated_concurrent_pending_requests = match config.request_mode_config {
            RequestModeConfig::ClosedLoop(parallel_requests) => parallel_requests,
            RequestModeConfig::OpenLoop(req_interval, interval_batch) => {
                ((MS_IN_SEC * interval_batch) as f64 / req_interval as f64).ceil() as usize
            }
        };
        Self {
            pending_requests: HashMap::with_capacity(estimated_concurrent_pending_requests),
            request_count: 0,
            response_count: 0,
            latency_avg: 0.,
            latency_m2: 0.,
            client_start_time: None,
            client_end_time: None,
            first_request_time: None,
            last_response_time: None,
        }
    }

    fn experiment_start(&mut self) {
        self.client_start_time = Some(Utc::now().timestamp_micros());
    }
    fn experiment_end(&mut self) {
        self.client_end_time = Some(Utc::now().timestamp_micros());
    }

    #[inline]
    fn new_request(&mut self, command_id: CommandId) {
        let request_time = Instant::now();
        self.pending_requests.insert(command_id, request_time);
        self.request_count += 1;
        if self.first_request_time.is_none() {
            self.first_request_time = Some(request_time);
        }
    }

    #[inline]
    fn new_batch_request(&mut self, start_id: CommandId, num_requests: usize) {
        let request_time = Instant::now();
        for command_id in start_id..start_id + num_requests {
            self.pending_requests.insert(command_id, request_time);
            self.request_count += 1;
        }
        if self.first_request_time.is_none() {
            self.first_request_time = Some(request_time);
        }
    }

    #[inline]
    fn new_response(&mut self, command_id: CommandId) {
        let response_time = Instant::now();
        self.last_response_time = Some(response_time);
        let request_time = self
            .pending_requests
            .remove(&command_id)
            .expect("Should never get a response without a request");
        let latency = (response_time - request_time).as_micros() as f64;
        self.response_count += 1;
        // Update moving statistics
        let delta = latency - self.latency_avg;
        self.latency_avg += delta / self.response_count as f64;
        let delta2 = latency - self.latency_avg;
        self.latency_m2 += delta * delta2;
    }

    fn save_summary(
        &self,
        client_config: ClientConfig,
        server_info: MetronomeConfigInfo,
    ) -> Result<(), std::io::Error> {
        let summary_filepath = client_config.summary_filename.clone();
        let (request_latency_average, request_latency_std_dev) = self.avg_response_latency();
        let summary = ClientSummary {
            client_config,
            server_info,
            client_start_time: self
                .client_start_time
                .expect("Should create summary after experiment start"),
            throughput: self.throughput(),
            total_responses: self.total_responses(),
            unanswered_requests: self.request_count - self.response_count,
            total_time: self.total_time(),
            request_latency_average,
            request_latency_std_dev,
        };
        let json_summary = serde_json::to_string_pretty(&summary)?;
        eprintln!("{json_summary}");
        let mut summary_file = File::create(summary_filepath)?;
        summary_file.write_all(json_summary.as_bytes())?;
        summary_file.flush()?;
        Ok(())
    }

    fn to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        let _file = File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path)?;
        Ok(())
    }

    fn total_responses(&self) -> usize {
        self.response_count
    }

    // The (average, std dev) response latency of all requests in milliseconds
    fn avg_response_latency(&self) -> (f64, f64) {
        let micros_in_ms = 1_000 as f64;
        let std_dev = (self.latency_m2 / self.response_count as f64).sqrt();
        (self.latency_avg / micros_in_ms, std_dev / micros_in_ms)
    }

    // The response throughput in responses/sec
    fn throughput(&self) -> f64 {
        let experiment_duration = self
            .client_end_time
            .expect("Should calculate throughput after experiment end")
            - self
                .client_start_time
                .expect("Should calculate throughput after experiment start");
        let throughput = (self.response_count as f64 / experiment_duration as f64) * 1_000_000.;
        return throughput;
    }

    // The total duration of request time in seconds
    fn total_time(&self) -> f64 {
        let first_request_time = self
            .first_request_time
            .expect("There should be some requests");
        let last_response_time = self.last_response_time.expect("There should be responses");
        (last_response_time - first_request_time).as_secs_f64()
    }
}
