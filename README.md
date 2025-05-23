# Array vs B-tree Performance Comparison

This project implements and compares the performance of Array and B-tree data structures for basic operations like search, insert, update, and delete. The comparison is done using various dataset sizes and supports both sequential and concurrent operations.

## Features

- Data structure implementations:
  - Array-based storage with linear search
  - B-tree implementation (2-3 tree variant)
  
- Operations supported:
  - Search
  - Insert
  - Update
  - Delete

- Testing modes:
  - Sequential operations
  - Concurrent operations with configurable number of workers
  
- Performance metrics:
  - Average operation time
  - Minimum operation time
  - Maximum operation time
  - Total operation time
  - Operations per second (for concurrent mode)

- Result visualization:
  - Detailed JSON reports
  - Comparative bar charts
  - Progress tracking during benchmarks

## Requirements

- Python 3.x
- Required packages:
  ```
  pandas
  matplotlib
  numpy
  ```

## Project Structure

```
.
├── module/
│   ├── __init__.py
│   ├── btree.py           # B-tree implementation
│   ├── comparison.py      # Comparison logic
│   ├── data_config.py     # Data generation config
│   └── result_exporter.py # Results export handling
├── data/                  # Test datasets
├── Results/              # Benchmark results
├── main.py              # Main program
└── README.md
```

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd btree-array-comparison
   ```

2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the program:
```bash
python main.py
```

The program offers two main modes:

1. **Run Benchmark**
   - Select dataset size
   - Choose between sequential or concurrent operations
   - Configure number of operations and workers
   - Results are automatically saved to the Results directory

2. **Direct Test**
   - Test individual operations
   - View detailed timing for each operation
   - Compare results between Array and B-tree
   - Results are saved for each test

## Dataset Format

The program expects CSV files with the following fields:
- Index (Primary key)
- Customer Id
- First Name
- Last Name
- Company
- City
- Country
- Phone 1
- Phone 2
- Email
- Subscription Date
- Website

## Results

Results are stored in the `Results` directory with the following structure:
- JSON file with detailed metrics
- PNG file with comparative bar chart
- Timestamp-based directory naming for easy tracking

## Performance Considerations

- Array performance:
  - O(n) for search, delete operations
  - O(1) for insert operations
  - Linear scaling with dataset size

- B-tree performance:
  - O(log n) for all operations
  - Better scaling with large datasets
  - More complex implementation

## Contributing

Feel free to submit issues and enhancement requests!
