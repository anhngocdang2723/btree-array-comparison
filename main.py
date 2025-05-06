from module.comparison import DataStructureComparison
import time
import os
from module.result_exporter import ResultExporter

def run_comparison(file_path, operations=1000, concurrent=False, max_workers=4):
    print(f"\nRunning comparison with dataset: {os.path.basename(file_path)}")
    print("=" * 50)
    
    start_time = time.time()
    comparison = DataStructureComparison()
    
    # Load data
    print("Loading data...")
    comparison.load_data(file_path)
    load_time = time.time() - start_time
    print(f"Data loading time: {load_time:.2f} seconds")
    
    # Run benchmarks
    print("\nRunning benchmarks...")
    results = comparison.benchmark_operations(operations, concurrent, max_workers)
    
    # Export results
    exporter = ResultExporter()
    dataset_info = {
        "file_path": file_path,
        "size": len(comparison.array_data),
        "fields": comparison.data_columns
    }
    benchmark_config = {
        "concurrent": concurrent,
        "operations": operations,
        "max_workers": max_workers
    }
    result_file = exporter.export_benchmark_results(dataset_info, benchmark_config, results)
    print(f"\nResults exported to: {result_file}")
    
    return results

def select_dataset():
    """Let user select a dataset from available files"""
    datasets = [
        "data/customers-1000.csv",
        "data/customers-10000.csv",
        "data/customers-100000.csv",
        "data/customers-1000000.csv",
        "data/customers-2000000.csv"
    ]
    
    print("\nAvailable datasets:")
    for i, dataset in enumerate(datasets, 1):
        if os.path.exists(dataset):
            print(f"{i}. {os.path.basename(dataset)}")
    
    while True:
        try:
            choice = int(input("\nSelect dataset number: "))
            if 1 <= choice <= len(datasets):
                selected = datasets[choice - 1]
                if os.path.exists(selected):
                    return selected
                else:
                    print("Selected dataset file not found!")
            else:
                print("Invalid selection!")
        except ValueError:
            print("Please enter a valid number!")

def get_benchmark_options():
    """Get benchmark configuration from user"""
    print("\nBenchmark Options:")
    print("1. Sequential (one operation at a time)")
    print("2. Concurrent (multiple operations simultaneously)")
    
    while True:
        try:
            choice = int(input("\nSelect benchmark mode (1-2): "))
            if choice in [1, 2]:
                concurrent = (choice == 2)
                max_workers = 4  # Default value
                
                if concurrent:
                    try:
                        workers = int(input("Enter number of concurrent workers (default=4): ") or "4")
                        if workers > 0:
                            max_workers = workers
                    except ValueError:
                        print("Using default value of 4 workers")
                
                try:
                    ops = int(input("Enter number of operations (press Enter for default=1000): ") or "1000")
                    if ops > 0:
                        return concurrent, max_workers, ops
                except ValueError:
                    print("Using default value of 1000 operations")
                
                return concurrent, max_workers, 1000
            else:
                print("Invalid choice!")
        except ValueError:
            print("Please enter a valid number!")

def direct_test():
    """Run direct testing of operations"""
    # Select dataset
    file_path = select_dataset()
    comparison = DataStructureComparison()
    exporter = ResultExporter()
    
    print("\nLoading data...")
    comparison.load_data(file_path)
    print("Data loaded successfully!")
    print(f"Dataset size: {len(comparison.array_data)} records")
    print(f"Fields: {', '.join(comparison.data_columns)}")
    
    while True:
        print("\nSelect operation:")
        print("1. Insert")
        print("2. Update")
        print("3. Delete")
        print("4. Search")
        print("5. Return to main menu")
        
        try:
            choice = int(input("\nEnter your choice (1-5): "))
            
            if choice == 5:
                break
                
            if choice not in [1, 2, 3, 4]:
                print("Invalid choice!")
                continue
            
            # Get key for operation
            key = int(input("Enter key (integer): "))
            
            if choice == 1:  # Insert
                # Generate new data with all fields
                value = comparison.generate_test_data(key)
                print("\nGenerated data for insert:")
                for field, val in value.items():
                    print(f"{field}: {val}")
                
                print("\nPerforming insert operation...")
                start_time = time.perf_counter()
                comparison.array_insert(key, value)
                array_time = time.perf_counter() - start_time
                
                start_time = time.perf_counter()
                comparison.btree.insert(key, value)
                btree_time = time.perf_counter() - start_time
                
                results = {
                    "array_time": array_time,
                    "btree_time": btree_time,
                    "data": value
                }
                
                # Export results
                dataset_info = {
                    "file_path": file_path,
                    "size": len(comparison.array_data),
                    "fields": comparison.data_columns
                }
                result_file = exporter.export_direct_test_results(dataset_info, "insert", key, results)
                print(f"\nResults exported to: {result_file}")
                
                print("\nResults:")
                print(f"Array insert time: {array_time:.6f} seconds")
                print(f"B-tree insert time: {btree_time:.6f} seconds")
                
            elif choice == 2:  # Update
                # Generate new data for update
                new_value = comparison.generate_test_data(key)
                new_value['updated'] = True
                print("\nGenerated data for update:")
                for field, val in new_value.items():
                    print(f"{field}: {val}")
                
                print("\nPerforming update operation...")
                start_time = time.perf_counter()
                success_array = comparison.array_update(key, new_value)
                array_time = time.perf_counter() - start_time
                
                start_time = time.perf_counter()
                success_btree = comparison.btree.update(key, new_value)
                btree_time = time.perf_counter() - start_time
                
                results = {
                    "array_time": array_time,
                    "btree_time": btree_time,
                    "success_array": success_array,
                    "success_btree": success_btree,
                    "data": new_value
                }
                
                # Export results
                dataset_info = {
                    "file_path": file_path,
                    "size": len(comparison.array_data),
                    "fields": comparison.data_columns
                }
                result_file = exporter.export_direct_test_results(dataset_info, "update", key, results)
                print(f"\nResults exported to: {result_file}")
                
                print("\nResults:")
                print(f"Array update time: {array_time:.6f} seconds")
                print(f"B-tree update time: {btree_time:.6f} seconds")
                print(f"Update successful in array: {success_array}")
                print(f"Update successful in B-tree: {success_btree}")
                
            elif choice == 3:  # Delete
                print("\nPerforming delete operation...")
                start_time = time.perf_counter()
                success_array = comparison.array_delete(key)
                array_time = time.perf_counter() - start_time
                
                start_time = time.perf_counter()
                success_btree = comparison.btree.delete(key)
                btree_time = time.perf_counter() - start_time
                
                results = {
                    "array_time": array_time,
                    "btree_time": btree_time,
                    "success_array": success_array,
                    "success_btree": success_btree
                }
                
                # Export results
                dataset_info = {
                    "file_path": file_path,
                    "size": len(comparison.array_data),
                    "fields": comparison.data_columns
                }
                result_file = exporter.export_direct_test_results(dataset_info, "delete", key, results)
                print(f"\nResults exported to: {result_file}")
                
                print("\nResults:")
                print(f"Array delete time: {array_time:.6f} seconds")
                print(f"B-tree delete time: {btree_time:.6f} seconds")
                print(f"Delete successful in array: {success_array}")
                print(f"Delete successful in B-tree: {success_btree}")
                
            elif choice == 4:  # Search
                print("\nPerforming search operation...")
                start_time = time.perf_counter()
                array_result = comparison.array_search(key)
                array_time = time.perf_counter() - start_time
                
                start_time = time.perf_counter()
                btree_result = comparison.btree.search(key)
                btree_time = time.perf_counter() - start_time
                
                results = {
                    "array_time": array_time,
                    "btree_time": btree_time,
                    "array_result": array_result,
                    "btree_result": btree_result
                }
                
                # Export results
                dataset_info = {
                    "file_path": file_path,
                    "size": len(comparison.array_data),
                    "fields": comparison.data_columns
                }
                result_file = exporter.export_direct_test_results(dataset_info, "search", key, results)
                print(f"\nResults exported to: {result_file}")
                
                print("\nResults:")
                print(f"Array search time: {array_time:.6f} seconds")
                print(f"B-tree search time: {btree_time:.6f} seconds")
                
                if array_result:
                    print("\nFound in array:")
                    for field, val in array_result.items():
                        print(f"{field}: {val}")
                else:
                    print("Not found in array")
                    
                if btree_result:
                    print("\nFound in B-tree:")
                    for field, val in btree_result.items():
                        print(f"{field}: {val}")
                else:
                    print("Not found in B-tree")
                
        except ValueError:
            print("Please enter valid numbers!")
        except Exception as e:
            print(f"An error occurred: {str(e)}")

def main():
    while True:
        print("\n=== Data Structure Comparison Tool ===")
        print("1. Run Benchmark")
        print("2. Direct Test")
        print("3. Exit")
        
        try:
            choice = int(input("\nEnter your choice (1-3): "))
            
            if choice == 1:
                # Select dataset
                file_path = select_dataset()
                
                # Get benchmark options
                concurrent, max_workers, operations = get_benchmark_options()
                
                # Run benchmark
                results = run_comparison(file_path, operations, concurrent, max_workers)
                
            elif choice == 2:
                direct_test()
                
            elif choice == 3:
                print("\nGoodbye!")
                break
                
            else:
                print("Invalid choice!")
                
        except ValueError:
            print("Please enter a valid number!")

if __name__ == "__main__":
    main()
