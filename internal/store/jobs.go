package store

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/lib/pq"
)

type Store struct {
	db *sql.DB
}

func NewStore(connStr string) (*Store, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &Store{db: db}, nil
}

type Job struct {
	ID             string
	Type           string
	Payload        []byte
	State          string
	Attempts       int
	MaxAttempts    int
	AvailableAt    time.Time
	StartedAt      sql.NullTime
	LeaseExpiresAt sql.NullTime
	LastError      sql.NullString
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (s *Store) ClaimJob(ctx context.Context, leaseDuration time.Duration) (*Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	query := `
	WITH next_job AS (
		SELECT id
		FROM jobs
		WHERE state = 'pending'
		AND available_at <= NOW()
		ORDER BY available_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	UPDATE jobs
	SET
		state = 'processing',
		attempts = attempts + 1,
		started_at = NOW(),
		lease_expires_at = NOW() + ($1 * INTERVAL '1 second'),
		updated_at = NOW()
	FROM next_job
	WHERE jobs.id = next_job.id
	RETURNING
		jobs.id,
		jobs.type,
		jobs.payload,
		jobs.state,
		jobs.attempts,
		jobs.max_attempts,
		jobs.available_at,
		jobs.started_at,
		jobs.lease_expires_at,
		jobs.last_error,
		jobs.created_at,
		jobs.updated_at;
	`

	row := tx.QueryRowContext(ctx, query, int(leaseDuration.Seconds()))

	var job Job
	err = row.Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&job.State,
		&job.Attempts,
		&job.MaxAttempts,
		&job.AvailableAt,
		&job.StartedAt,
		&job.LeaseExpiresAt,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil // No job available
	}
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &job, nil
}

func (s *Store) MarkSucceeded(ctx context.Context, jobID string) error {
	query := `
	UPDATE jobs
	SET
		state = 'succeeded',
		lease_expires_at = NULL,
		updated_at = NOW()
	WHERE id = $1
		AND state = 'processing';
	`

	res, err := s.db.ExecContext(ctx, query, jobID)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 {
		return sql.ErrNoRows
	}

	return nil
}

func (s *Store) MarkRetry(ctx context.Context, jobID string, errMsg string, delay time.Duration) error {
	query := `
	UPDATE jobs
	SET
		state = 'pending',
		available_at = NOW() + ($2 * INTERVAL '1 second'),
		lease_expires_at = NULL,
		last_error = $3,
		updated_at = NOW()
	WHERE id = $1
		AND state = 'processing';
	`

	res, err := s.db.ExecContext(ctx, query, jobID, int(delay.Seconds()), errMsg)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 {
		return sql.ErrNoRows
	}

	return nil
}

func (s *Store) MarkFailed(ctx context.Context, jobID string, errMsg string) error {
	query := `
	UPDATE jobs
	SET
		state = 'failed',
		lease_expires_at = NULL,
		last_error = $2,
		updated_at = NOW()
	WHERE id = $1
		AND state = 'processing';
	`

	res, err := s.db.ExecContext(ctx, query, jobID, errMsg)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows != 1 {
		return sql.ErrNoRows
	}

	return nil
}

func (s *Store) ReapExpiredJobs(ctx context.Context) (int64, error) {
	query := `
	UPDATE jobs
	SET
		state = 'pending',
		available_at = NOW(),
		lease_expires_at = NULL,
		updated_at = NOW()
	WHERE state = 'processing'
		AND lease_expires_at <= NOW();
	`
	
	res, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}

	return res.RowsAffected()
}
