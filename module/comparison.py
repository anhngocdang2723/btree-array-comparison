import time
import pandas as pd
from module.btree import BTree
import concurrent.futures
import threading
from module.data_config import generate_record

class DataStructureComparison:
    def __init__(self):
        self.array_data = []
        self.btree = BTree(t=3)  # t=3 for a 2-3 tree
        self.data_columns = None  # Store column names
        self.array_lock = threading.Lock()  # Lock for array operations

    def load_data(self, file_path):
        """Load data from CSV file"""
        df = pd.read_csv(file_path)
        self.data_columns = df.columns.tolist()
        
        # Convert DataFrame to list of dictionaries with proper data types
        for _, row in df.iterrows():
            # Convert row to dictionary with proper data types
            record = {}
            for col in self.data_columns:
                value = row[col]
                # Convert numeric strings to appropriate type
                if pd.api.types.is_numeric_dtype(df[col]):
                    value = float(value) if '.' in str(value) else int(value)
                record[col] = value
            
            key = record[self.data_columns[0]]  # First column as key
            self.array_data.append((key, record))
            self.btree.insert(key, record)

    def generate_test_data(self, key):
        """Generate test data using the data configuration"""
        return generate_record(key)

    def array_search(self, key):
        """Search in array using linear search"""
        for k, v in self.array_data:
            if k == key:
                return v
        return None

    def array_insert(self, key, value):
        """Insert into array with data validation"""
        # Validate that all required fields are present
        if not all(col in value for col in self.data_columns):
            raise ValueError("Missing required fields in data")
        self.array_data.append((key, value))

    def array_delete(self, key):
        """Delete from array"""
        for i, (k, _) in enumerate(self.array_data):
            if k == key:
                self.array_data.pop(i)
                return True
        return False

    def array_update(self, key, new_value):
        """Update value in array with data validation"""
        # Validate that all required fields are present
        if not all(col in new_value for col in self.data_columns):
            raise ValueError("Missing required fields in data")
            
        for i, (k, _) in enumerate(self.array_data):
            if k == key:
                self.array_data[i] = (key, new_value)
                return True
        return False

    def benchmark_concurrent_operations(self, operations=100, max_workers=4):
        """Benchmark operations with concurrent execution
        Args:
            operations: Number of operations to perform
            max_workers: Maximum number of concurrent workers
        """
        results = {
            'array': {'search': [], 'insert': [], 'update': [], 'delete': []},
            'btree': {'search': [], 'insert': [], 'update': [], 'delete': []}
        }

        existing_keys = [k for k, _ in self.array_data]
        
        print("\nRunning concurrent benchmarks...")
        print("=" * 50)
        print(f"Dataset size: {len(self.array_data)} records")
        print(f"Number of operations: {operations}")
        print(f"Number of concurrent workers: {max_workers}")
        print(f"Fields: {', '.join(self.data_columns)}")

        def array_search_worker(key):
            with self.array_lock:
                start_time = time.perf_counter()
                result = self.array_search(key)
                end_time = time.perf_counter()
                return end_time - start_time

        def btree_search_worker(key):
            start_time = time.perf_counter()
            result = self.btree.search(key)
            end_time = time.perf_counter()
            return end_time - start_time

        def array_insert_worker(i):
            key = max(existing_keys) + i + 1
            value = self.generate_test_data(key)
            with self.array_lock:
                start_time = time.perf_counter()
                self.array_insert(key, value)
                end_time = time.perf_counter()
                return end_time - start_time

        def btree_insert_worker(i):
            key = max(existing_keys) + i + 1
            value = self.generate_test_data(key)
            start_time = time.perf_counter()
            self.btree.insert(key, value)
            end_time = time.perf_counter()
            return end_time - start_time

        def array_update_worker(key):
            new_value = self.generate_test_data(key)
            new_value['updated'] = True
            with self.array_lock:
                start_time = time.perf_counter()
                self.array_update(key, new_value)
                end_time = time.perf_counter()
                return end_time - start_time

        def btree_update_worker(key):
            new_value = self.generate_test_data(key)
            new_value['updated'] = True
            start_time = time.perf_counter()
            self.btree.update(key, new_value)
            end_time = time.perf_counter()
            return end_time - start_time

        def array_delete_worker(key):
            with self.array_lock:
                start_time = time.perf_counter()
                self.array_delete(key)
                end_time = time.perf_counter()
                return end_time - start_time

        def btree_delete_worker(key):
            start_time = time.perf_counter()
            self.btree.delete(key)
            end_time = time.perf_counter()
            return end_time - start_time

        # Benchmark concurrent search
        print("\nBenchmarking Concurrent Search Operations:")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Array search
            array_futures = [executor.submit(array_search_worker, existing_keys[i % len(existing_keys)]) 
                           for i in range(operations)]
            results['array']['search'] = [f.result() for f in array_futures]

            # B-tree search
            btree_futures = [executor.submit(btree_search_worker, existing_keys[i % len(existing_keys)]) 
                           for i in range(operations)]
            results['btree']['search'] = [f.result() for f in btree_futures]

        # Benchmark concurrent insert
        print("\nBenchmarking Concurrent Insert Operations:")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Array insert
            array_futures = [executor.submit(array_insert_worker, i) 
                           for i in range(operations)]
            results['array']['insert'] = [f.result() for f in array_futures]

            # B-tree insert
            btree_futures = [executor.submit(btree_insert_worker, i) 
                           for i in range(operations)]
            results['btree']['insert'] = [f.result() for f in btree_futures]

        # Benchmark concurrent update
        print("\nBenchmarking Concurrent Update Operations:")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Array update
            array_futures = [executor.submit(array_update_worker, existing_keys[i % len(existing_keys)]) 
                           for i in range(operations)]
            results['array']['update'] = [f.result() for f in array_futures]

            # B-tree update
            btree_futures = [executor.submit(btree_update_worker, existing_keys[i % len(existing_keys)]) 
                           for i in range(operations)]
            results['btree']['update'] = [f.result() for f in btree_futures]

        # Benchmark concurrent delete
        print("\nBenchmarking Concurrent Delete Operations:")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Array delete
            array_futures = [executor.submit(array_delete_worker, existing_keys[i % len(existing_keys)]) 
                           for i in range(operations)]
            results['array']['delete'] = [f.result() for f in array_futures]

            # B-tree delete
            btree_futures = [executor.submit(btree_delete_worker, existing_keys[i % len(existing_keys)]) 
                           for i in range(operations)]
            results['btree']['delete'] = [f.result() for f in btree_futures]

        # Calculate and print detailed statistics
        print("\nDetailed Results (Concurrent Operations):")
        print("=" * 50)
        for structure in results:
            print(f"\n{structure.upper()} Structure:")
            for operation in results[structure]:
                times = results[structure][operation]
                if times:  # Check if we have any results
                    avg_time = sum(times) / len(times)
                    min_time = min(times)
                    max_time = max(times)
                    total_time = sum(times)
                    ops_per_second = len(times) / total_time if total_time > 0 else 0
                    
                    print(f"{operation.capitalize()}:")
                    print(f"  Average time: {avg_time:.9f} seconds")
                    print(f"  Minimum time: {min_time:.9f} seconds")
                    print(f"  Maximum time: {max_time:.9f} seconds")
                    print(f"  Total time: {total_time:.9f} seconds")
                    print(f"  Operations per second: {ops_per_second:.2f}")
                else:
                    print(f"{operation.capitalize()}: No results available")

        return results

    def benchmark_operations(self, operations=None, concurrent=False, max_workers=4):
        """Benchmark operations for both data structures
        Args:
            operations: Number of operations to perform. If None, use 10% of dataset size
            concurrent: Whether to run operations concurrently
            max_workers: Maximum number of concurrent workers (if concurrent=True)
        """
        if concurrent:
            return self.benchmark_concurrent_operations(operations, max_workers)
            
        # If operations not specified, use 10% of dataset size
        if operations is None:
            operations = max(100, len(self.array_data) // 10)
        
        results = {
            'array': {'search': [], 'insert': [], 'update': [], 'delete': []},
            'btree': {'search': [], 'insert': [], 'update': [], 'delete': []}
        }

        # Get existing keys from dataset
        existing_keys = [k for k, _ in self.array_data]
        
        print("\nRunning benchmarks...")
        print("=" * 50)
        print(f"Dataset size: {len(self.array_data)} records")
        print(f"Number of operations: {operations}")
        print(f"Fields: {', '.join(self.data_columns)}")

        # Benchmark search
        print("\nBenchmarking Search Operations:")
        for i in range(operations):
            if i % 100 == 0:
                print(f"Progress: {i}/{operations} operations")
            
            # Use existing keys for search
            key = existing_keys[i % len(existing_keys)]
            
            # Array search
            start_time = time.perf_counter()
            self.array_search(key)
            end_time = time.perf_counter()
            results['array']['search'].append(end_time - start_time)

            # B-tree search
            start_time = time.perf_counter()
            self.btree.search(key)
            end_time = time.perf_counter()
            results['btree']['search'].append(end_time - start_time)

        # Benchmark insert
        print("\nBenchmarking Insert Operations:")
        for i in range(operations):
            if i % 100 == 0:
                print(f"Progress: {i}/{operations} operations")
                
            # Generate new key that doesn't exist
            while True:
                key = max(existing_keys) + i + 1
                if key not in existing_keys:
                    break
            
            value = self.generate_test_data(key)

            # Array insert
            start_time = time.perf_counter()
            self.array_insert(key, value)
            end_time = time.perf_counter()
            results['array']['insert'].append(end_time - start_time)

            # B-tree insert
            start_time = time.perf_counter()
            self.btree.insert(key, value)
            end_time = time.perf_counter()
            results['btree']['insert'].append(end_time - start_time)

        # Benchmark update
        print("\nBenchmarking Update Operations:")
        for i in range(operations):
            if i % 100 == 0:
                print(f"Progress: {i}/{operations} operations")
                
            # Use existing keys for update
            key = existing_keys[i % len(existing_keys)]
            new_value = self.generate_test_data(key)
            new_value['updated'] = True

            # Array update
            start_time = time.perf_counter()
            self.array_update(key, new_value)
            end_time = time.perf_counter()
            results['array']['update'].append(end_time - start_time)

            # B-tree update
            start_time = time.perf_counter()
            self.btree.update(key, new_value)
            end_time = time.perf_counter()
            results['btree']['update'].append(end_time - start_time)

        # Benchmark delete
        print("\nBenchmarking Delete Operations:")
        for i in range(operations):
            if i % 100 == 0:
                print(f"Progress: {i}/{operations} operations")
                
            # Use existing keys for delete
            key = existing_keys[i % len(existing_keys)]
            
            # Array delete
            start_time = time.perf_counter()
            self.array_delete(key)
            end_time = time.perf_counter()
            results['array']['delete'].append(end_time - start_time)

            # B-tree delete
            start_time = time.perf_counter()
            self.btree.delete(key)
            end_time = time.perf_counter()
            results['btree']['delete'].append(end_time - start_time)

        # Calculate and print detailed statistics
        print("\nDetailed Results:")
        print("=" * 50)
        for structure in results:
            print(f"\n{structure.upper()} Structure:")
            for operation in results[structure]:
                times = results[structure][operation]
                avg_time = sum(times) / len(times)
                min_time = min(times)
                max_time = max(times)
                print(f"{operation.capitalize()}:")
                print(f"  Average time: {avg_time:.9f} seconds")
                print(f"  Minimum time: {min_time:.9f} seconds")
                print(f"  Maximum time: {max_time:.9f} seconds")
                print(f"  Total time: {sum(times):.9f} seconds")

        return results 