package pgchan

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const outputPlugin = "pgoutput"

type TableEvent struct {
	CommitTimestamp time.Time
	Tables          []string
	LSN             string
}

func newEvent(commitTimestamp time.Time, lsn pglogrepl.LSN, transactionRelations []uint32, relations map[uint32]relation) TableEvent {
	slices.Sort(transactionRelations)
	transactionRelations = slices.Compact(transactionRelations)
	tableNames := make([]string, 0, len(transactionRelations))

	var missingRelationIDs []uint32
	for _, id := range transactionRelations {
		rel, ok := relations[id]
		if !ok {
			missingRelationIDs = append(missingRelationIDs, id)
			continue
		}
		tableNames = append(tableNames, rel.Name)
	}
	slices.Sort(tableNames)
	tableNames = slices.Compact(tableNames)

	slog.Debug("new event", "commit_timestamp", commitTimestamp, "tables_length", len(tableNames), "lsn", lsn)
	for _, id := range missingRelationIDs { // these should not happen
		slog.Error("missing relations", "missing_relation_id", id, "lsn", lsn)
	}

	return TableEvent{CommitTimestamp: commitTimestamp, Tables: tableNames, LSN: lsn.String()}
}

func TableEvents(ctx context.Context, databaseURL string, c chan<- TableEvent, tableNames ...string) error {
	databaseURLConfig, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return err
	}
	databaseURL, err = ensureDatabaseReplication(databaseURL)
	if err != nil {
		return err
	}

	nTables := len(tableNames)

	return runWithDBConnection(ctx, databaseURL, func(ctx context.Context, conn *pgconn.PgConn) error {
		pubName, slotName := newPublicationName(databaseURLConfig), newSlotName(databaseURLConfig)
		sys, err := ensureSlotAndPublicationExist(ctx, conn, pubName, slotName, tableNames...)
		if err != nil {
			return err
		}

		var (
			transactionRelations = make([]uint32, 0, min(max(nTables, 10), 200)) // some arbitrary min/max values
			commitTimestamp      time.Time
			relations            = make(map[uint32]relation, nTables)
		)

		return readMessages(ctx, sys.XLogPos, conn, func(ctx context.Context, pos pglogrepl.LSN, message pglogrepl.Message) error {
			slog.Debug("read message", "type", reflect.TypeOf(message).String())
			if len(transactionRelations)+1 == cap(transactionRelations) {
				slices.Sort(transactionRelations)
				transactionRelations = slices.Compact(transactionRelations)
			}
			switch msg := message.(type) {
			case *pglogrepl.StreamStartMessageV2:
				commitTimestamp = time.Now() // should be over-written by BEGIN message or COMMIT
			case *pglogrepl.StreamStopMessageV2:
				c <- newEvent(commitTimestamp, pos, transactionRelations, relations)
				transactionRelations = transactionRelations[:0]
				commitTimestamp = time.Time{}
			case *pglogrepl.RelationMessageV2:
				if _, ok := relations[msg.RelationID]; ok {
					return nil
				}
				relations[msg.RelationID] = relation{
					Name:      msg.RelationName,
					Namespace: msg.Namespace,
				}
			case *pglogrepl.BeginMessage:
				// if there is a previous relation transaction being processed, let's send it
				if len(transactionRelations) > 0 {
					c <- newEvent(commitTimestamp, pos, transactionRelations, relations)
					transactionRelations = transactionRelations[:0]
					commitTimestamp = time.Time{}
				}
			case *pglogrepl.StreamCommitMessageV2:
				commitTimestamp = msg.CommitTime
				if len(transactionRelations) > 0 {
					c <- newEvent(commitTimestamp, pos, transactionRelations, relations)
					transactionRelations = transactionRelations[:0]
					commitTimestamp = time.Time{}
				}
			case *pglogrepl.CommitMessage:
				commitTimestamp = msg.CommitTime
				if len(transactionRelations) > 0 {
					c <- newEvent(commitTimestamp, pos, transactionRelations, relations)
					transactionRelations = transactionRelations[:0]
					commitTimestamp = time.Time{}
				}
			case *pglogrepl.InsertMessageV2:
				transactionRelations = append(transactionRelations, msg.RelationID)
			case *pglogrepl.UpdateMessageV2:
				transactionRelations = append(transactionRelations, msg.RelationID)
			case *pglogrepl.DeleteMessageV2:
				transactionRelations = append(transactionRelations, msg.RelationID)
			case *pglogrepl.TruncateMessageV2:
				transactionRelations = append(transactionRelations, msg.RelationIDs...)
			case *pglogrepl.TypeMessageV2:
			case *pglogrepl.OriginMessage:
			case *pglogrepl.LogicalDecodingMessageV2:
			case *pglogrepl.StreamAbortMessageV2:
				transactionRelations = transactionRelations[:0]
				commitTimestamp = time.Time{}
			default:
				slog.Error("unexpected message type", "type", reflect.TypeOf(message).String())
			}
			return nil
		})
	})
}

type relation struct {
	Name      string
	Namespace string
}

func readMessages(ctx context.Context, pos pglogrepl.LSN, conn *pgconn.PgConn, handleEvent func(ctx context.Context, pos pglogrepl.LSN, message pglogrepl.Message) error) error {
	var (
		standbyMessageTimeout = time.Second * 10
		standbyDeadline       = time.Now().Add(standbyMessageTimeout)
		inStream              = false
	)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		replyRequested, err := receiveMessage(ctx, conn, standbyDeadline, pos, inStream, func(ctx context.Context, message pglogrepl.Message) error {
			switch msg := message.(type) {
			case *pglogrepl.StreamStartMessageV2:
				inStream = true
			case *pglogrepl.StreamStopMessageV2:
				inStream = false
			case *pglogrepl.StreamCommitMessageV2:
				pos = msg.TransactionEndLSN
			}
			return handleEvent(ctx, pos, message)
		})
		if err != nil {
			return err
		}
		if replyRequested || time.Now().After(standbyDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pos, ClientTime: time.Now()})
			if err != nil {
				slog.Error("failed to send standby status update", "error", err, "lsn", pos)
			}
			standbyDeadline = time.Now().Add(standbyMessageTimeout)
			if replyRequested {
				slog.Debug("sent status update", "lsn", pos, "next_deadline", standbyDeadline, "requested", true)
			} else {
				slog.Debug("sent status update", "lsn", pos, "next_deadline", standbyDeadline)
			}
		}
	}
}

func receiveMessage(ctx context.Context, conn *pgconn.PgConn, deadline time.Time, clientXLogPos pglogrepl.LSN, inStream bool, handleMessage func(ctx context.Context, message pglogrepl.Message) error) (bool, error) {
	deadlineCtx, cancel := context.WithDeadline(ctx, deadline)
	rawMsg, err := conn.ReceiveMessage(deadlineCtx)
	cancel()
	if err != nil {
		if pgconn.Timeout(err) {
			return true, nil
		}
		slog.Error("failed to receive message", "error", err, "lsn", clientXLogPos)
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		log.Fatalf("received Postgres WAL error: %+v", errMsg)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		slog.Error("received unexpected message", "type", reflect.TypeOf(msg).String())
		return true, nil
	}

	replyRequested := false
	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			slog.Error("failed to parse primary keepalive message", "error", err)
			return false, nil
		}
		if pkm.ServerWALEnd > clientXLogPos {
			clientXLogPos = pkm.ServerWALEnd
		}
		if pkm.ReplyRequested {
			replyRequested = true
		}
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			slog.Error("failed to parse transaction log data", "error", err)
			return false, nil
		}

		message, err := pglogrepl.ParseV2(xld.WALData, inStream)
		if err != nil {
			log.Fatalf("Parse logical replication message: %s", err)
		}

		if err := handleMessage(ctx, message); err != nil {
			return replyRequested, err
		}
	}
	return replyRequested, nil
}

func ensureSlotAndPublicationExist(ctx context.Context, conn *pgconn.PgConn, pubName, slotName string, tableNames ...string) (pglogrepl.IdentifySystemResult, error) {
	result := conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", pubName))
	if _, err := result.ReadAll(); err != nil {
		return pglogrepl.IdentifySystemResult{}, err
	}

	if len(tableNames) > 0 {
		tn := slices.Clone(tableNames)
		for i, n := range tn {
			tn[i] = strconv.Quote(n)
		}
		result = conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR %s;", pubName, strings.Join(tn, ", ")))
		if _, err := result.ReadAll(); err != nil {
			return pglogrepl.IdentifySystemResult{}, err
		}
	} else {
		result = conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", pubName))
		if _, err := result.ReadAll(); err != nil {
			return pglogrepl.IdentifySystemResult{}, err
		}
	}

	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return pglogrepl.IdentifySystemResult{}, fmt.Errorf("failed to identify system: %w", err)
	}
	slog.Info("system identification", "system_id", sys.SystemID, "timeline", sys.Timeline, "tx_log_position", sys.XLogPos, "db_name", sys.DBName)

	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", pubName),
	}

	if err := pglogrepl.DropReplicationSlot(ctx, conn, slotName, pglogrepl.DropReplicationSlotOptions{
		Wait: true,
	}); err != nil {
		slog.Error("failed to drop replication slot", "error", err)
	}

	if _, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{
		Temporary: true,
		Mode:      pglogrepl.LogicalReplication,
	}); err != nil {
		return pglogrepl.IdentifySystemResult{}, fmt.Errorf("replication slot creation failed: %w", err)
	}
	slog.Info("replication slot created", "slot_name", slotName)

	err = pglogrepl.StartReplication(context.Background(), conn, slotName, sys.XLogPos, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArguments,
		Mode:       pglogrepl.LogicalReplication,
	})
	if err != nil {
		return pglogrepl.IdentifySystemResult{}, fmt.Errorf("replication slot start failed: %w", err)
	}
	slog.Info("replication slot started", "slot_name", slotName)

	return sys, nil
}

func runWithDBConnection(ctx context.Context, databaseURL string, run func(ctx context.Context, conn *pgconn.PgConn) error) error {
	conn, err := pgconn.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL server: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		return fmt.Errorf("database ping failed: %w", err)
	}

	closeCtx := context.WithoutCancel(ctx)
	defer closeConnection(closeCtx, conn)

	return run(ctx, conn)
}

func closeConnection(ctx context.Context, conn *pgconn.PgConn) func() {
	return func() {
		if err := conn.Close(ctx); err != nil {
			slog.Error("failed to close pgx connection", "error", err)
		}
	}
}

func sanitizeName(name string) string {
	n := strings.TrimSpace(name)
	n = strings.ReplaceAll(n, "-", "_")
	n = strings.ReplaceAll(n, ".", "_")
	return n
}

func newSlotName(config *pgx.ConnConfig) string {
	return strings.Join([]string{cmp.Or(sanitizeName(config.Database), "postgres"), "slot"}, "_")
}

func newPublicationName(config *pgx.ConnConfig) string {
	return strings.Join([]string{cmp.Or(sanitizeName(config.Database), "postgres"), "pub"}, "_")
}

func ensureDatabaseReplication(databaseURL string) (string, error) {
	u, err := url.Parse(databaseURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	if q.Get("replication") != "database" {
		q.Set("replication", "database")
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
