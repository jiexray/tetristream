#!/usr/bin/env python
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import matplotlib

METRICS_LOG_FILE = "metric_log_files/root-taskexecutor-0-flink-1-metrics.log"
ROCKSDB_METRICS_LOG_FILE = "metric_log_files/rocksdb-metrics.log"
REQUIRED_METRICS = "required_metrics_1.txt"
LINE_SEQ = '!'
START_TS = 2000
END_TS = 3000
TICK_INTERVAL = 40

class MetricDraw:

    def __init__(self, metric_filters, log_file, x_tick_interval=10, scale=False):
        self.metric_filters = []
        for metric_filter in metric_filters.split("|"):
            self.metric_filters.append(metric_filter.split(','))
        # self.metric_filters = metric_filters
        print("metric_filters: ", self.metric_filters)
        self.log_file = log_file
        self.full_name_to_val = {}
        self.full_name_to_extract_name = {}
        self.full_name_to_timeline = {}
        self.scale = scale
        self.x_tick_interval = x_tick_interval

    def extract_simple_val(self, val, metric_full_name):
        if metric_full_name.find(',') == -1:
            return metric_full_name, float(val)
        else:
            tm_idx = metric_full_name.find("taskmanager")
            return metric_full_name[tm_idx:].split('.')[3][:15], float(val)

    # find the first filter in metric_filters as the metric_name in cfstats
    def extract_cfstats_val(self, cfstats_info, metric_full_name):
        cfstats_metric = self.metric_filters[0].split(':')[1]
        metric_name_idx = cfstats_info.find(cfstats_metric)
        if metric_name_idx == -1:
            return metric_full_name.split('.')[4][:15], 0

        metric_value_start_idx = cfstats_info[metric_name_idx:].find('=') + 1
        metric_value_end_idx = cfstats_info[metric_name_idx:].find(',') if cfstats_info[metric_name_idx:].find(',') != -1 else cfstats_info[metric_name_idx:].find('}')
        metric_value = cfstats_info[metric_name_idx:][metric_value_start_idx: metric_value_end_idx]
        return metric_full_name.split('.')[4][:15], float(metric_value)

    def is_filter(self, metric_full_name):
        is_filtered = True
        for metric_filter in self.metric_filters:
            is_matched_current_filter = True
            for filter_element in metric_filter:
                main_filter_element = filter_element.split(';')[0]
                if main_filter_element not in metric_full_name:
                    is_matched_current_filter = False
                    break
            if is_matched_current_filter:
                is_filtered = False
                break
        return is_filtered

    def filter_metrics(self):
        with open(self.log_file) as log_file_reader:
            lines = log_file_reader.readlines()
            for line in lines:
                if len(line.split(LINE_SEQ)) != 3:
                    continue

                metric_full_name = line.split(LINE_SEQ)[0].strip()

                isFilter = self.is_filter(metric_full_name)
                if isFilter:
                    continue

                metrics_ts = int(line.split(LINE_SEQ)[2].strip())
                metric_val = line.split(LINE_SEQ)[1].strip()
                if "cfstats" not in metric_full_name:
                    extract_name, extract_val = self.extract_simple_val(metric_val, metric_full_name)
                else:
                    extract_name, extract_val = self.extract_cfstats_val(metric_val, metric_full_name)

                self.full_name_to_extract_name[metric_full_name] = extract_name

                if metric_full_name in self.full_name_to_val:
                    self.full_name_to_val[metric_full_name].append(extract_val)
                    self.full_name_to_timeline[metric_full_name].append(metrics_ts)
                else:
                    self.full_name_to_val[metric_full_name] = [extract_val]
                    self.full_name_to_timeline[metric_full_name] = [metrics_ts]

    def plot_metrics(self):
        import matplotlib.pyplot as plt
        font = {'family': 'normal', 'weight': 'bold', 'size': 30}
        matplotlib.rc('font', **font)

        if len(self.full_name_to_val.keys()) == 0:
            return

        if self.scale:
            for vals in self.full_name_to_val.values():
                if max(vals) == 0:
                    continue
                max_val = max(vals)
                for i in range(len(vals)):
                    vals[i] = vals[i] / max_val

        plt.figure(figsize=(40, 20))
        plt.title("metric: " + str(self.metric_filters))
        plt.xlabel("Time")
        plt.ylabel(self.metric_filters[0])
        labels = []

        min_ts = -1
        for metrics_timestamps in self.full_name_to_timeline.values():
            if min_ts == -1:
                min_ts = min(metrics_timestamps)
            else:
                min_ts = min(min_ts, min(metrics_timestamps))

        for full_name, vals in self.full_name_to_val.items():
            print("metrics_full_name: " + full_name + ", vals length: " + str(len(vals)) + ", timeline length: " + str(len(self.full_name_to_timeline[full_name])) + ", start ts: " + str(min_ts))
            labels.append(self.full_name_to_extract_name[full_name])
            x_vals = [(x - min_ts) / 1000 for x in self.full_name_to_timeline[full_name]]

            # select only data from START_TS to END_TS
            if START_TS != -1 or END_TS != -1:
                selected_x_vals = []
                selected_vals = []
                for i in range(len(x_vals)):
                    if START_TS <= x_vals[i] <= END_TS:
                        selected_x_vals.append(x_vals[i])
                        selected_vals.append(vals[i])

                selected_x_vals = [x - START_TS for x in selected_x_vals]
                plt.plot(selected_x_vals, selected_vals, linewidth=3.0)
                plt.xticks(range(0, END_TS - START_TS + 10, self.x_tick_interval))
            else:
                plt.plot(x_vals, vals, linewidth=3.0)
                plt.xticks(range(min(x_vals), max(x_vals) + 10, self.x_tick_interval))

        plt.legend(labels)
        plt.show()

    def draw(self):
        print("draw metric: " + str(self.metric_filters) + ", in " + self.log_file)
        self.filter_metrics()
        self.plot_metrics()


def main():
    print("Visualize metrics log file: " + METRICS_LOG_FILE)
    with open(REQUIRED_METRICS, "r") as required_metrics_reader:
        for line in required_metrics_reader.readlines():
            if line.find('#') != -1:
                continue
            if line.find("|") != -1:
                drawer = MetricDraw(line.strip(), METRICS_LOG_FILE, TICK_INTERVAL, True)
            else:
                drawer = MetricDraw(line.strip(), METRICS_LOG_FILE, TICK_INTERVAL)
            drawer.draw()

    # with open(REQUIRED_METRICS, "r") as required_metrics_reader:
    #     for line in required_metrics_reader.readlines():
    #         if line.find('#') != -1:
    #             continue
    #
    #         drawer = MetricDraw(line.strip().split(','), ROCKSDB_METRICS_LOG_FILE)
    #         drawer.draw()

if __name__ == '__main__':
    main()
