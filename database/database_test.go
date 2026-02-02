package database

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Reset singleton between tests
func resetDB() {
	dbInstance = nil
}

// Start Postgres test container
func mustStartPostgresContainer() (func(context.Context, ...testcontainers.TerminateOption) error, error) {
	ctx := context.Background()

	dbName := "database"
	dbUser := "user"
	dbPwd := "password"

	container, err := postgres.Run(
		ctx,
		"postgres:latest",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPwd),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second),
		),
	)
	if err != nil {
		return nil, err
	}

	hostIP, err := container.Host(ctx)
	if err != nil {
		return container.Terminate, err
	}

	mappedPort, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return container.Terminate, err
	}

	// Assign database globals
	database = dbName
	username = dbUser
	password = dbPwd
	host = hostIP
	port = mappedPort.Port()
	schema = "public"

	return container.Terminate, nil
}

// Test lifecycle controller
func TestMain(m *testing.M) {
	teardown, err := mustStartPostgresContainer()
	if err != nil {
		log.Fatalf("failed to start postgres container: %v", err)
	}

	code := m.Run()

	if teardown != nil {
		_ = teardown(context.Background())
	}

	os.Exit(code)
}

// ---------- Tests ----------

func TestNew(t *testing.T) {
	resetDB()

	srv := New()

	if srv == nil {
		t.Fatal("New() returned nil")
	}
}

func TestHealth(t *testing.T) {
	resetDB()

	srv := New()

	stats := srv.Health()

	if stats["status"] != "up" {
		t.Fatalf("expected status 'up', got '%s'", stats["status"])
	}

	if _, exists := stats["error"]; exists {
		t.Fatalf("unexpected error returned: %v", stats["error"])
	}

	if stats["message"] == "" {
		t.Fatal("expected health message to be present")
	}

	if _, ok := stats["open_connections"]; !ok {
		t.Fatal("expected connection stats to be present")
	}
}

func TestClose(t *testing.T) {
	resetDB()

	srv := New()

	err := srv.Close()
	if err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
}
