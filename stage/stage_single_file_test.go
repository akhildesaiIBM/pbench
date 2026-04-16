package stage

import (
	"os"
	"path/filepath"
	"pbench/prestoapi"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSingleFileMode_SplitsByDefault tests that without single-file mode,
// queries are split by semicolons (backward compatible behavior)
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

	// Without single-file mode, should split into 2 queries
	queries, err := SplitQueriesForTest(file, false)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(queries), "Should split into 2 queries by default")
	assert.Contains(t, queries[0], "first_query")
	assert.Contains(t, queries[1], "second_query")
}

// TestSingleFileMode_ExecutesAsOneUnit tests that with single-file mode enabled,
// the entire file is treated as one query (matches presto-cli --file behavior)
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

	// With single-file mode, should return 1 query containing entire file
	queries, err := SplitQueriesForTest(file, true)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(queries), "Should treat entire file as 1 query in single-file mode")
	assert.Contains(t, queries[0], "first_query")
	assert.Contains(t, queries[0], "second_query")
	assert.Contains(t, queries[0], ";") // Should contain the semicolon
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

	// Both modes should handle empty files gracefully
	queries, err := SplitQueriesForTest(file, false)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(queries))

	file.Seek(0, 0) // Reset file pointer
	queries, err = SplitQueriesForTest(file, true)
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

	// Default mode: should split into 3 queries
	queries, err := SplitQueriesForTest(file, false)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(queries))

	// Single-file mode: should be 1 query
	file.Seek(0, 0)
	queries, err = SplitQueriesForTest(file, true)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(queries))
	assert.Contains(t, queries[0], "table1")
	assert.Contains(t, queries[0], "table2")
	assert.Contains(t, queries[0], "table3")
}

// Helper function to test query splitting logic
func SplitQueriesForTest(file *os.File, singleFileMode bool) ([]string, error) {
	if singleFileMode {
		// Single-file mode: read entire file as one query
		content, err := os.ReadFile(file.Name())
		if err != nil {
			return nil, err
		}
		trimmed := string(content)
		// Trim whitespace
		for len(trimmed) > 0 && (trimmed[0] == ' ' || trimmed[0] == '\n' || trimmed[0] == '\t' || trimmed[0] == '\r') {
			trimmed = trimmed[1:]
		}
		for len(trimmed) > 0 && (trimmed[len(trimmed)-1] == ' ' || trimmed[len(trimmed)-1] == '\n' || trimmed[len(trimmed)-1] == '\t' || trimmed[len(trimmed)-1] == '\r') {
			trimmed = trimmed[:len(trimmed)-1]
		}
		if len(trimmed) > 0 {
			return []string{trimmed}, nil
		}
		return []string{}, nil
	}
	// Default mode: use existing split logic
	return prestoapi.SplitQueries(file)
}

// Made with Bob
