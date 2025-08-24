# gogress

**gogress** is a lightweight Go-based file storage database with a simple write-ahead log (WAL) mechanism. It supports table creation, basic key-value operations, and crash recovery by replaying log files.

### Example with CLI
```bash
# Start the CLI
gogress-cli list-tables
cars
new_table
users
default

# create a table
gogress-cli create-table products

# create a record
gogress-cli put products sku123 hammer

# get a record
gogress-cli get products sku123
hammer
```

## Project Structure

- **Main Application**  
  Root-level [main.go](main.go) launches the application.

- **Command-Line Interface (CLI)**  
  The `cli` directory contains CLI-specific code and tests ([cli/main.go](cli/main.go), [cli/main_test.go](cli/main_test.go)).

- **Database Implementation**  
  The core database logic is implemented under the [pkg/db](pkg/db) folder:
  - [`db.NewDB`](pkg/db/db.go): Initializes a new database instance.
  - [`db.Table`](pkg/db/table.go): Provides methods like `Put` and `Get` to store and retrieve records.
  - Write-ahead logging and crash recovery is managed in [`db.BuildIndex`](pkg/db/db.go).

- **Tests**  
  Unit tests cover various components:
  - Main application tests in [main_test.go](main_test.go).
  - Database tests in [pkg/db/db_test.go](pkg/db/db_test.go) and [pkg/db/table_test.go](pkg/db/table_test.go).

## Getting Started

### Prerequisites

- [Go](https://go.dev) (version 1.16 or later)
- A Unix-like environment to support file system operations

### Installation

Clone the repository and navigate to the project folder:

```
git clone github.com/igomez10/gogress
cd gogress
```

### Build

Build the project using `go build`:

```
go build -o gogress-cli cli/main.go
```

### Running Tests

Run all tests with:

```
go test ./...
```

Or test specific packages:

```
go test ./pkg/db
```

## Usage

The database is initialized in [`db.NewDB`](pkg/db/db.go) and can be used to perform key-value operations. The CLI (located in the `cli` directory) currently supports commands such as listing tables ([see TestListTables in cli/main_test.go](cli/main_test.go)).

Examples of operations include:

- **Put a Record:**  
  Refer to [`db.Table.Put`](pkg/db/table.go) for adding a new record.

- **Get a Record:**  
  See [`db.Table.Get`](pkg/db/table.go) for retrieving stored values.

## Storage and Recovery

- **Storage Files:**  
  Data is stored in files under `/tmp/gogress/` (as set in [`db.initializeStorage`](pkg/db/db.go)).  
- **Crash Recovery:**  
  A write-ahead log (WAL) is maintained and used to rebuild the in-memory index via [`db.BuildIndex`](pkg/db/db.go).

## Contributing

Feel free to fork the project and submit pull requests. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is provided without any warranty. See the LICENSE file
