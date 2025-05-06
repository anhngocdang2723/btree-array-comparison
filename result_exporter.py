import os
import json
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

class ResultExporter:
    def __init__(self):
        self.results_dir = "Results"
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)

    def _create_result_directory(self, dataset_name, mode):
        """Create a directory for the current benchmark results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dir_name = f"{dataset_name}_{mode}_{timestamp}"
        result_path = os.path.join(self.results_dir, dir_name)
        os.makedirs(result_path)
        return result_path, timestamp

    def _create_comparison_plot(self, results, dataset_name, mode, result_path):
        """Create comparison plot for benchmark results"""
        # Prepare data for plotting
        operations = ['Insert', 'Update', 'Delete', 'Search']
        array_times = []
        btree_times = []
        
        for op in operations:
            op_lower = op.lower()
            if op_lower in results['array'] and op_lower in results['btree']:
                array_times.append(results['array'][op_lower]['average_time'])
                btree_times.append(results['btree'][op_lower]['average_time'])

        # Create bar chart
        x = np.arange(len(operations))
        width = 0.35

        fig, ax = plt.subplots(figsize=(10, 6))
        rects1 = ax.bar(x - width/2, array_times, width, label='Array')
        rects2 = ax.bar(x + width/2, btree_times, width, label='B-tree')

        # Add labels and title
        ax.set_ylabel('Average Time (seconds)')
        ax.set_title(f'Performance Comparison: {dataset_name} ({mode})')
        ax.set_xticks(x)
        ax.set_xticklabels(operations)
        ax.legend()

        # Add value labels on top of bars
        def autolabel(rects):
            for rect in rects:
                height = rect.get_height()
                ax.annotate(f'{height:.6f}',
                           xy=(rect.get_x() + rect.get_width()/2, height),
                           xytext=(0, 3),  # 3 points vertical offset
                           textcoords="offset points",
                           ha='center', va='bottom')

        autolabel(rects1)
        autolabel(rects2)

        # Adjust layout and save
        plt.tight_layout()
        plot_filename = os.path.join(result_path, "comparison_plot.png")
        plt.savefig(plot_filename)
        plt.close()

        return plot_filename

    def export_benchmark_results(self, dataset_info, benchmark_config, results):
        """Export benchmark results to a JSON file"""
        # Prepare the data structure
        export_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "dataset_info": {
                "name": os.path.basename(dataset_info["file_path"]),
                "size": dataset_info["size"],
                "fields": dataset_info["fields"]
            },
            "benchmark_config": {
                "mode": "concurrent" if benchmark_config["concurrent"] else "sequential",
                "operations": benchmark_config["operations"],
                "max_workers": benchmark_config.get("max_workers", 1)
            },
            "results": {
                "array": {},
                "btree": {}
            }
        }

        # Process results for each structure
        for structure in results:
            for operation in results[structure]:
                times = results[structure][operation]
                if times:
                    export_data["results"][structure][operation] = {
                        "average_time": sum(times) / len(times),
                        "min_time": min(times),
                        "max_time": max(times),
                        "total_time": sum(times),
                        "operations_per_second": len(times) / sum(times) if sum(times) > 0 else 0,
                        "total_operations": len(times)
                    }

        # Create result directory and save files
        dataset_name = os.path.basename(dataset_info["file_path"])
        mode = "concurrent" if benchmark_config["concurrent"] else "sequential"
        result_path, timestamp = self._create_result_directory(dataset_name, mode)
        
        # Save JSON results
        json_filename = os.path.join(result_path, "benchmark_results.json")
        with open(json_filename, 'w') as f:
            json.dump(export_data, f, indent=4)

        # Create and save comparison plot
        plot_filename = self._create_comparison_plot(export_data["results"], dataset_name, mode, result_path)
        print(f"\nResults saved in directory: {result_path}")
        print(f"- JSON results: {json_filename}")
        print(f"- Comparison plot: {plot_filename}")

        return result_path

    def export_direct_test_results(self, dataset_info, operation, key, results):
        """Export direct test results to a JSON file"""
        # Prepare the data structure
        export_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "dataset_info": {
                "name": os.path.basename(dataset_info["file_path"]),
                "size": dataset_info["size"],
                "fields": dataset_info["fields"]
            },
            "test_info": {
                "operation": operation,
                "key": key
            },
            "results": results
        }

        # Create result directory
        dataset_name = os.path.basename(dataset_info["file_path"])
        result_path, timestamp = self._create_result_directory(dataset_name, f"direct_test_{operation}")
        
        # Save JSON results
        json_filename = os.path.join(result_path, "test_results.json")
        with open(json_filename, 'w') as f:
            json.dump(export_data, f, indent=4)

        print(f"\nTest results saved in directory: {result_path}")
        print(f"- JSON results: {json_filename}")

        return result_path 