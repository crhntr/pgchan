package pgchan_test

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/crhntr/pgchan"
)

func TestOneTable(t *testing.T) {
	ctx := t.Context()
	dbURL := setupPostgresDocker(t, "pg_chan_all_tables")
	conn, err := pgx.Connect(ctx, dbURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	_, err = conn.Exec(ctx, `CREATE TABLE some_table (
		id    serial PRIMARY KEY,
		value text   NOT NULL
	)`)

	listenerCtx, listenerCancel := context.WithCancel(ctx)

	c := make(chan pgchan.TableEvent)
	wg := errgroup.Group{}
	wg.Go(func() error {
		defer close(c)
		return pgchan.TableEvents(listenerCtx, dbURL, c)
	})
	wg.Go(func() error {
		ticker := time.NewTicker(time.Second / 10)
		deadline := time.After(time.Second * 10)
		value := 1
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-deadline:
				return fmt.Errorf("timed out waiting for table events")
			case <-c:
				listenerCancel()
				return nil // ok, table event received
			case <-ticker.C:
				if _, err := conn.Exec(ctx, `INSERT INTO some_table (value) VALUES ($1);`, strconv.Itoa(value)); err != nil {
					return err
				}
				value++
			}
		}
	})
	require.NoError(t, wg.Wait())
}

func TestSomeTables(t *testing.T) {
	ctx := t.Context()
	dbURL := setupPostgresDocker(t, "pg_chan_some_tables")
	conn, err := pgx.Connect(ctx, dbURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	for _, sql := range []string{
		`CREATE TABLE some_other_table (id serial PRIMARY KEY, value text NOT NULL)`,
		`CREATE TABLE some_table (id serial PRIMARY KEY, value text NOT NULL)`,
	} {
		_, err = conn.Exec(ctx, sql)
		require.NoError(t, err)
	}

	listenerCtx, listenerCancel := context.WithCancel(ctx)

	all := make(chan pgchan.TableEvent)
	one := make(chan pgchan.TableEvent)
	wg := errgroup.Group{}
	wg.Go(func() error {
		defer close(all)
		return pgchan.TableEvents(listenerCtx, dbURL, all)
	})
	wg.Go(func() error {
		defer close(one)
		return pgchan.TableEvents(listenerCtx, dbURL, one, "some_table")
	})
	wg.Go(func() error {

		ticker := time.NewTicker(time.Second / 20)
		deadline := time.After(time.Second * 3)
		someEventCount, allTablesEventCount := 0, 0
		value := 1
		addToSomeTable := new(sync.Once)

		channelCount := 2
		for loop := 1; channelCount > 0; loop++ {
			// t.Logf("[for select] loop=%d value=%d some=%d all=%d", value, loop, allTablesEventCount, someEventCount)
			select {
			case <-deadline:
				t.Error("[<-deadline] timed out waiting for table events")
			case e, more := <-one:
				if !more {
					channelCount--
					continue
				}
				someEventCount++
				t.Logf("[<-one] table event [all: %d, some: %d]: %#v", allTablesEventCount, someEventCount, e)
			case e, more := <-all:
				if !more {
					channelCount--
					continue
				}
				allTablesEventCount++
				t.Logf("[<-all] table event: [all: %d, some: %d] %#v", allTablesEventCount, someEventCount, e)
				if allTablesEventCount > 5 && someEventCount >= 1 {
					listenerCancel()
				}
			case <-ticker.C:
				t.Logf("[<-ticker.C] insert into some_other_table: %d", value)
				insertValueIntoTable(t, conn, "some_other_table", value)
				if allTablesEventCount > 1 {
					addToSomeTable.Do(func() {
						t.Logf("[tick] insert into some_table: %d", value)
						insertValueIntoTable(t, conn, "some_table", value)
					})
				}
				value++
			}
		}
		return nil
	})
	require.NoError(t, wg.Wait())
}

func insertValueIntoTable(t *testing.T, conn *pgx.Conn, name string, value int) {
	t.Helper()
	ctx := context.Background()
	if _, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (value) VALUES ($1);`, name), strconv.Itoa(value)); err != nil {
		t.Error(err)
	}
}

// setupPostgresDocker pulls the PostgreSQL Docker image, stops/removes an existing container
// (if any), and then starts a new container with the given configuration.
func setupPostgresDocker(t *testing.T, containerName string) string {
	t.Helper()

	var (
		postgresUser     = cmp.Or(os.Getenv("POSTGRES_USER"), "pgchan_test_user")
		postgresPassword = cmp.Or(os.Getenv("POSTGRES_PASSWORD"), "pgchan_test_password")
		postgresDB       = cmp.Or(os.Getenv("DATABASE_NAME"), "pgchan_test")
		postgresImage    = cmp.Or(os.Getenv("POSTGRES_IMAGE"), "postgres")
		hostPort         = cmp.Or(os.Getenv("HOST_PORT"), freePort(t))
	)

	t.Logf("Using Docker container name: %s", containerName)
	t.Logf("Using PostgreSQL image: %s", postgresImage)

	// Pull the PostgreSQL Docker image.
	t.Logf("Pulling PostgreSQL Docker image %q...", postgresImage)
	if output, err := exec.Command("docker", "pull", postgresImage).CombinedOutput(); err != nil {
		t.Fatalf("failed to pull image %q: %v\nOutput: %s", postgresImage, err, string(output))
	}

	// Check if a container with the specified name already exists.
	out, err := exec.Command("docker", "ps", "-aq", "-f", fmt.Sprintf("name=%s", containerName)).Output()
	if err != nil {
		t.Fatalf("failed to check for existing container %q: %v", containerName, err)
	}

	if containerID := strings.TrimSpace(string(out)); containerID != "" {
		t.Logf("A container named %q already exists.", containerName)

		// If the container is running, stop it.
		checkRunningCmd := exec.Command("docker", "ps", "-aq", "-f", "status=running", "-f", fmt.Sprintf("name=%s", containerName))
		runningOut, err := checkRunningCmd.Output()
		if err != nil {
			t.Fatalf("failed to check if container %q is running: %v", containerName, err)
		}
		if runningID := strings.TrimSpace(string(runningOut)); runningID != "" {
			t.Logf("Stopping the running container %q...", containerName)
			if output, err := exec.Command("docker", "stop", containerName).CombinedOutput(); err != nil {
				t.Fatalf("failed to stop container %q: %v\nOutput: %s", containerName, err, string(output))
			}
		}

		// Remove the existing container.
		t.Logf("Removing the existing container %q...", containerName)
		if output, err := exec.Command("docker", "rm", containerName).CombinedOutput(); err != nil {
			t.Fatalf("failed to remove container %q: %v\nOutput: %s", containerName, err, string(output))
		}
	}

	// Run the new container.
	runArgs := []string{
		"run", "-d",
		"--name", containerName,
		"-p", fmt.Sprintf("%s:5432", hostPort),
		"-e", fmt.Sprintf("POSTGRES_USER=%s", postgresUser),
		"-e", fmt.Sprintf("POSTGRES_PASSWORD=%s", postgresPassword),
		"-e", fmt.Sprintf("POSTGRES_DB=%s", postgresDB),
		postgresImage,
		"postgres", "-c", "wal_level=logical",
	}
	t.Logf("Starting a new container %q...", containerName)
	if output, err := exec.Command("docker", runArgs...).CombinedOutput(); err != nil {
		t.Fatalf("failed to start container %q: %v\nOutput: %s", containerName, err, string(output))
	}

	t.Logf("Container %q is set up and running.", containerName)

	connStr := fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable",
		postgresUser, postgresPassword, hostPort, postgresDB)

	if err := ensureDBConnection(t, connStr); err != nil {
		t.Fatal(err)
	}

	t.Logf("PostgreSQL connection string: %s", connStr)
	return connStr
}

func ensureDBConnection(t *testing.T, connStr string) error {
	t.Helper()
	var connErr error
	for try := 1; try <= 5; try++ {
		time.Sleep(time.Second * time.Duration(try-1))
		if err := tryConnectAndPing(connStr); err != nil {
			connErr = err
			continue
		}
		return nil
	}
	return connErr
}

func tryConnectAndPing(connStr string) error {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return err
	}
	if err := conn.Ping(ctx); err != nil {
		return err
	}
	return nil
}

func freePort(t *testing.T) string {
	t.Helper()

	// Listen on a random port.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to acquire a free port: %v", err)
	}
	defer closeAndLogError(t, l)

	// Extract the port number from the listener's address.
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("failed to cast listener address to *net.TCPAddr")
	}

	return strconv.Itoa(addr.Port)
}

func closeAndLogError(t *testing.T, closer io.Closer) {
	t.Helper()
	if err := closer.Close(); err != nil {
		t.Logf("failed to close connection: %v", err)
	}
}
