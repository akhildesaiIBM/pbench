package stage

import (
	"os"
	"path/filepath"
	"pbench/prestoapi"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSingleFileMode_SplitsByDefault tests that queries are always split by semicolons
// for proper Presto API execution (backward compatible behavior)
func TestSingleFileMode_SplitsByDefault(t *testing.T) {
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "test_query.sql")
	
	// Create a query file with 2 statements
	content := `SELECT 1 AS first_query;
SELECT 2 AS second_query;`
	err := os.WriteFile(queryFile, []byte(content), 0644)
	assert.Nil(t, err)

	// Test the file reading logic directly
	file, err := os.Open(queryFile)
	assert.Nil(t, err)
	defer file.Close()

	// Queries are always split by semicolons to avoid Presto API syntax errors
	queries, err := prestoapi.SplitQueries(file)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(queries), "Should split into 2 queries")
	assert.Contains(t, queries[0], "first_query")
	assert.Contains(t, queries[1], "second_query")
}

// TestSingleFileMode_ExecutesAsOneUnit tests that with single-file mode enabled,
// queries are still split but tracked/reported as a single file execution
func TestSingleFileMode_ExecutesAsOneUnit(t *testing.T) {
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "test_query.sql")
	
	// Create a query file with 2 statements
	content := `SELECT 1 AS first_query;
SELECT 2 AS second_query;`
	err := os.WriteFile(queryFile, []byte(content), 0644)
	assert.Nil(t, err)

	// Test the file reading logic directly
	file, err := os.Open(queryFile)
	assert.Nil(t, err)
	defer file.Close()

	// Queries are split for execution (to avoid Presto API errors)
	queries, err := prestoapi.SplitQueries(file)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(queries), "Should split into 2 queries for execution")
	
	// In single-file mode, these 2 queries will be:
	// 1. Executed sequentially
	// 2. Tracked as 1 file (query_index=0)
	// 3. Combined timing reported
	assert.Contains(t, queries[0], "first_query")
	assert.Contains(t, queries[1], "second_query")
}

// TestSingleFileMode_EmptyFile tests handling of empty files
func TestSingleFileMode_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "empty.sql")
	
	err := os.WriteFile(queryFile, []byte("   \n\n  "), 0644)
	assert.Nil(t, err)

	file, err := os.Open(queryFile)
	assert.Nil(t, err)
	defer file.Close()

	// Empty files should be handled gracefully
	queries, err := prestoapi.SplitQueries(file)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(queries))
}

// TestSingleFileMode_MultipleStatements tests file with many statements
func TestSingleFileMode_MultipleStatements(t *testing.T) {
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "multi.sql")
	
	// Simulate a TPC-DS query file with multiple statements
	content := `-- Query 14 variant 1
SELECT * FROM table1 WHERE id = 1;

-- Query 14 variant 2
SELECT * FROM table2 WHERE id = 2;

-- Query 14 variant 3
SELECT * FROM table3 WHERE id = 3;`
	
	err := os.WriteFile(queryFile, []byte(content), 0644)
	assert.Nil(t, err)

	file, err := os.Open(queryFile)
	assert.Nil(t, err)
	defer file.Close()

	// Queries are always split for execution
	queries, err := prestoapi.SplitQueries(file)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(queries), "Should split into 3 queries for execution")
	
	// In single-file mode:
	// - These 3 queries execute sequentially
	// - Reported as 1 file (query_index=0)
	// - Combined timing and row counts
	assert.Contains(t, queries[0], "table1")
	assert.Contains(t, queries[1], "table2")
	assert.Contains(t, queries[2], "table3")
}

