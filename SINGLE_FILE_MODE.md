# Single-File Execution Mode

## Overview

The `--single-file-mode` flag allows pbench to execute entire query files as a single unit, matching the behavior of `presto-cli --file`. This is particularly useful for TPC-DS benchmarks where query files may contain multiple SQL statements that should be counted as one query.

## Motivation

### Problem
By default, pbench splits query files by semicolons (`;`) and executes each statement separately. For example, a file like `q14.sql` containing:

```sql
-- Query 14 variant 1
SELECT * FROM table1;

-- Query 14 variant 2  
SELECT * FROM table2;
```

Would be counted as **2 queries** (query_index 0 and 1).

### Solution
With `--single-file-mode`, the entire file is executed as **1 query**, matching how `presto-cli --file q14.sql` behaves. This ensures:
- Correct query counts for TPC-DS compliance (99 queries, not 103)
- Timing matches SPS (Single Performance Specification) execution method
- Behavior consistent with presto-cli

## Usage

### Command Line

```bash
# Run TPC-DS with single-file mode
pbench run tpc-ds.json --single-file-mode

# Run with other flags
pbench run tpc-ds.json \
    --single-file-mode \
    --server http://localhost:8080 \
    --catalog tpcds \
    --schema sf1
```

### Stage JSON Configuration

The flag is passed through command-line only and applies to all stages in the run.

## Behavior Comparison

### Default Mode (Backward Compatible)

```bash
pbench run tpc-ds.json
```

**Query File:** `q14.sql` (2 statements)
```sql
SELECT * FROM table1;
SELECT * FROM table2;
```

**Result:**
- Executes: 2 separate queries
- Query indices: 0, 1
- File count: 1
- Query count: 2

### Single-File Mode

```bash
pbench run tpc-ds.json --single-file-mode
```

**Query File:** `q14.sql` (2 statements)
```sql
SELECT * FROM table1;
SELECT * FROM table2;
```

**Result:**
- Executes: 1 query (entire file)
- Query index: 0
- File count: 1
- Query count: 1
- Timing: Covers execution of both statements (like presto-cli)

## TPC-DS Example

### Without Single-File Mode (Incorrect)

```bash
pbench run tpc-ds.json
```

**Result:**
- 99 query files
- 103 query executions (some files have multiple statements)
- ❌ Does not match TPC-DS specification

### With Single-File Mode (Correct)

```bash
pbench run tpc-ds.json --single-file-mode
```

**Result:**
- 99 query files
- 99 query executions (one per file)
- ✅ Matches TPC-DS specification
- ✅ Matches presto-cli --file behavior

## Timing Behavior

### Single-File Mode Timing

When executing a file with multiple statements:

```
start_time = when query execution starts
# Execute statement 1
# Execute statement 2
# ... all statements in file
end_time = when all statements complete
elapsed_time = end_time - start_time
```

This matches the SPS (Single Performance Specification) execution method where:
- `presto-cli --file q14.sql` reads the entire file
- Executes all statements sequentially
- Returns single timing for the entire file

## Implementation Details

### Code Changes

1. **Command-line flag** (`cmd/run.go`):
   ```go
   runCmd.Flags().BoolVar(&run.SingleFileMode, "single-file-mode", false, 
       "Execute entire query file as single unit (do not split by semicolons)")
   ```

2. **State propagation** (`stage/states.go`):
   ```go
   type SharedStageStates struct {
       // ...
       SingleFileMode bool
   }
   ```

3. **Query parsing** (`stage/stage.go`):
   ```go
   if s.States.SingleFileMode {
       // Read entire file as one query
       content, _ := io.ReadAll(file)
       queries = []string{string(content)}
   } else {
       // Default: split by semicolons
       queries, _ = prestoapi.SplitQueries(file)
   }
   ```

### Backward Compatibility

- **Default behavior unchanged**: Without the flag, pbench splits by semicolons (existing behavior)
- **Opt-in feature**: Users must explicitly enable with `--single-file-mode`
- **No breaking changes**: All existing benchmarks continue to work

## Testing

Unit tests verify both modes:

```bash
# Run single-file mode tests
go test ./stage -run TestSingleFileMode -v
```

Tests cover:
- Default mode splits by semicolons
- Single-file mode treats file as one unit
- Empty file handling
- Multiple statement files

## Use Cases

### 1. TPC-DS Benchmarks
Ensure exactly 99 queries are executed (file count, not statement count).

### 2. Presto-CLI Compatibility
Match the behavior of `presto-cli --file` for consistent benchmarking.

### 3. Complex Query Files
Execute multi-statement query files as atomic units (e.g., setup + query + cleanup).

## Migration Guide

### Existing TPC-DS Benchmarks

**Before:**
```bash
pbench run tpc-ds.json
# Result: 103 queries (incorrect)
```

**After:**
```bash
pbench run tpc-ds.json --single-file-mode
# Result: 99 queries (correct)
```

### Jenkins Integration

Update your Jenkinsfile to use the new flag:

```groovy
stage('Run TPC-DS') {
    steps {
        sh '''
            ./pbench run tpc-ds.json \
                --single-file-mode \
                --server ${PRESTO_SERVER} \
                --catalog tpcds \
                --schema sf1000
        '''
    }
}
```

## FAQ

**Q: Does this change existing behavior?**  
A: No, it's opt-in via `--single-file-mode` flag. Default behavior is unchanged.

**Q: Should I always use single-file mode?**  
A: Use it when you want file-based counting (like TPC-DS) or need to match presto-cli --file behavior.

**Q: What about query files with no semicolons?**  
A: Both modes handle them identically (single query).

**Q: Does this affect timing?**  
A: Yes, single-file mode reports one timing for the entire file (all statements combined), matching presto-cli behavior.

**Q: Can I use this with random execution?**  
A: Yes, the flag works with all pbench features including random execution, warm runs, etc.

## See Also

- [TPC-DS Specification](http://www.tpc.org/tpcds/)
- [Presto CLI Documentation](https://prestodb.io/docs/current/installation/cli.html)
- [PBench Run Command](https://github.com/prestodb/pbench/wiki/The-Run-Command)